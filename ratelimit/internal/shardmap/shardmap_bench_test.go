package shardmap

import (
	"fmt"
	"sync"
	"testing"
)

// singleLockMap models the pre-§3.1 design: one global RWMutex over one map.
// It exists only so the benchmark can quantify the contention the sharded Map
// removes.
type singleLockMap[V any] struct {
	mu sync.RWMutex
	m  map[string]*V
}

func newSingleLockMap[V any]() *singleLockMap[V] {
	return &singleLockMap[V]{m: make(map[string]*V)}
}

func (s *singleLockMap[V]) getOrCreate(key string, create func() *V) *V {
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		return v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok = s.m[key]; ok {
		return v
	}
	v = create()
	s.m[key] = v
	return v
}

const keysPerG = 512

// BenchmarkSingleLock_ParallelManyKeys is the "before" (unsharded) baseline.
func BenchmarkSingleLock_ParallelManyKeys(b *testing.B) {
	m := newSingleLockMap[int]()
	var gid int
	var gidMu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		gidMu.Lock()
		gid++
		base := gid * keysPerG
		gidMu.Unlock()
		keys := make([]string, keysPerG)
		for i := range keys {
			keys[i] = fmt.Sprintf("k-%d", base+i)
		}
		i := 0
		for pb.Next() {
			v := i
			m.getOrCreate(keys[i%keysPerG], func() *int { return &v })
			i++
		}
	})
}

// BenchmarkSharded_ParallelManyKeys is the "after" (sharded) result.
func BenchmarkSharded_ParallelManyKeys(b *testing.B) {
	m := New[int]()
	var gid int
	var gidMu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		gidMu.Lock()
		gid++
		base := gid * keysPerG
		gidMu.Unlock()
		keys := make([]string, keysPerG)
		for i := range keys {
			keys[i] = fmt.Sprintf("k-%d", base+i)
		}
		i := 0
		for pb.Next() {
			v := i
			m.GetOrCreate(keys[i%keysPerG], func() *int { return &v })
			i++
		}
	})
}
