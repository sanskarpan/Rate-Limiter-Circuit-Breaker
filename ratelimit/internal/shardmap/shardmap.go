// Package shardmap provides a generic, sharded concurrent map used by the
// local rate-limit algorithms to hold per-key state.
//
// Every algorithm previously guarded its whole per-key map with a single
// sync.RWMutex. Under high key cardinality and concurrency that global lock is
// the dominant contention point: two requests for two *different* keys still
// serialize on the same mutex whenever a key is created and contend on the
// RLock even on the read fast path.
//
// A Map spreads keys across N independent shards (N = next power of two of
// 2×GOMAXPROCS), each with its own RWMutex and Go map. A key is routed to a
// shard by hashing it with the stdlib hash/maphash, so requests for different
// keys usually touch different shards and proceed without contending. The hash
// seed is process-random (maphash), which also removes any adversarial
// collision concern.
//
// Map holds *V pointers (the caller owns per-entry synchronization, exactly as
// before — each algorithm keeps its own per-entry mutex inside V). The Map only
// synchronizes creation, lookup, deletion, and iteration of the key→*V mapping.
//
// Zero external dependencies (hash/maphash and runtime are stdlib).
//
// All methods are safe for concurrent use.
package shardmap

import (
	"hash/maphash"
	"runtime"
	"sync"
)

// shard is one stripe of the map with its own lock.
type shard[V any] struct {
	mu sync.RWMutex
	m  map[string]*V
	// pad keeps hot shards from sharing a cache line with their neighbours,
	// avoiding false sharing under heavy concurrent writes. sync.RWMutex is
	// smaller than a cache line, so without padding two shards' locks can land
	// on the same line and ping-pong between cores.
	_ [64]byte
}

// Map is a sharded map from string keys to *V. The zero value is not usable;
// construct one with New.
type Map[V any] struct {
	seed   maphash.Seed
	mask   uint64 // len(shards)-1; len is always a power of two
	shards []*shard[V]
}

// New creates a sharded map sized to the current GOMAXPROCS.
//
// The shard count is the next power of two ≥ 2×GOMAXPROCS, clamped to a minimum
// of 1. A power-of-two count lets routing use a cheap bitmask instead of a
// modulo.
func New[V any]() *Map[V] {
	n := nextPow2(2 * runtime.GOMAXPROCS(0))
	if n < 1 {
		n = 1
	}
	shards := make([]*shard[V], n)
	for i := range shards {
		shards[i] = &shard[V]{m: make(map[string]*V)}
	}
	return &Map[V]{
		seed:   maphash.MakeSeed(),
		mask:   uint64(n - 1),
		shards: shards,
	}
}

// nextPow2 returns the smallest power of two ≥ n (and ≥ 1).
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// shardFor routes a key to its shard by hashing it.
func (s *Map[V]) shardFor(key string) *shard[V] {
	h := maphash.String(s.seed, key)
	return s.shards[h&s.mask]
}

// Get returns the value for key and whether it was present. It takes only a
// read lock on the key's shard.
func (s *Map[V]) Get(key string) (*V, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	v, ok := sh.m[key]
	sh.mu.RUnlock()
	return v, ok
}

// GetOrCreate returns the existing value for key, or creates one with create,
// stores it, and returns it. create is called at most once per successful
// insertion and only while the shard write lock is held; it must not call back
// into the same Map (that would deadlock on the shard lock).
//
// It uses the standard read-fast-path / write-double-check pattern so an
// already-present key is served under a read lock without blocking other
// shards or other keys on the same shard.
func (s *Map[V]) GetOrCreate(key string, create func() *V) *V {
	sh := s.shardFor(key)
	sh.mu.RLock()
	v, ok := sh.m[key]
	sh.mu.RUnlock()
	if ok {
		return v
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if v, ok = sh.m[key]; ok {
		return v
	}
	v = create()
	sh.m[key] = v
	return v
}

// Delete removes key from its shard.
func (s *Map[V]) Delete(key string) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	delete(sh.m, key)
	sh.mu.Unlock()
}

// Range calls fn for every key/value pair across all shards. It locks one shard
// at a time (never all at once), so it is a consistent snapshot per shard but
// not a global snapshot; concurrent mutations to other shards may or may not be
// observed. If fn returns false, iteration stops.
//
// fn must not call back into the same Map for a key in the shard currently being
// iterated (the shard lock is held during fn). Deleting the current key via a
// captured Delete is safe only if it targets a *different* shard; use
// DeleteMatching for lock-safe conditional deletion during a sweep.
func (s *Map[V]) Range(fn func(key string, v *V) bool) {
	for _, sh := range s.shards {
		if !s.rangeShard(sh, fn) {
			return
		}
	}
}

// rangeShard iterates a single shard under its read lock.
func (s *Map[V]) rangeShard(sh *shard[V], fn func(key string, v *V) bool) bool {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	for k, v := range sh.m {
		if !fn(k, v) {
			return false
		}
	}
	return true
}

// DeleteMatching sweeps every shard and deletes each entry for which pred
// returns true. pred is called under the shard write lock; it must not call
// back into the same Map. This is the sharded equivalent of the idle-eviction
// loops that previously ranged the single global map under a write lock.
func (s *Map[V]) DeleteMatching(pred func(key string, v *V) bool) {
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, v := range sh.m {
			if pred(k, v) {
				delete(sh.m, k)
			}
		}
		sh.mu.Unlock()
	}
}

// Len returns the total number of entries across all shards. It briefly
// read-locks each shard in turn, so the result is an approximate point-in-time
// count under concurrent mutation.
func (s *Map[V]) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// Shards returns the number of shards. Exposed for tests/benchmarks.
func (s *Map[V]) Shards() int { return len(s.shards) }
