package memcached

import (
	"strconv"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
)

// fakeMemcache is an in-process implementation of the `client` interface used by
// the store. It reproduces the memcached semantics the store relies on:
//
//   - Get / Set / Add / Delete
//   - Increment (unsigned, decimal-string counters; miss when absent)
//   - Gets/CompareAndSwap via a monotonic per-item CAS token
//
// It is deliberately NOT wired to a real network, so the unit tests exercise the
// full CAS-loop logic (including conflicts, injected via casHook) without a live
// server. It is safe for concurrent use.
type fakeMemcache struct {
	mu     sync.Mutex
	data   map[string]*fakeItem
	nextID uint64

	// failConn, when set, makes every op return a connection-style error so the
	// fallback path can be tested.
	failConn bool

	// casHook, if set, is invoked at the start of CompareAndSwap and can bump the
	// stored version to force a one-shot ErrCASConflict, simulating a concurrent
	// writer. It returns the number of remaining conflicts to inject.
	casHook func(key string)
}

type fakeItem struct {
	value   []byte
	casID   uint64
	exptime int32
}

func newFakeMemcache() *fakeMemcache {
	return &fakeMemcache{data: map[string]*fakeItem{}, nextID: 1}
}

var errFakeConn = &net_OpErr{}

// net_OpErr is a minimal error that reports Timeout()==false but is treated by
// isConnError as a connectivity failure (non-protocol error).
type net_OpErr struct{}

func (*net_OpErr) Error() string { return "fake: connection refused" }

func (f *fakeMemcache) Get(key string) (*memcache.Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return nil, errFakeConn
	}
	it, ok := f.data[key]
	if !ok {
		return nil, memcache.ErrCacheMiss
	}
	return &memcache.Item{Key: key, Value: append([]byte(nil), it.value...), Expiration: it.exptime, CasID: it.casID}, nil
}

func (f *fakeMemcache) Set(item *memcache.Item) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return errFakeConn
	}
	f.nextID++
	f.data[item.Key] = &fakeItem{value: append([]byte(nil), item.Value...), casID: f.nextID, exptime: item.Expiration}
	return nil
}

func (f *fakeMemcache) Add(item *memcache.Item) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return errFakeConn
	}
	if _, ok := f.data[item.Key]; ok {
		return memcache.ErrNotStored
	}
	f.nextID++
	f.data[item.Key] = &fakeItem{value: append([]byte(nil), item.Value...), casID: f.nextID, exptime: item.Expiration}
	return nil
}

func (f *fakeMemcache) CompareAndSwap(item *memcache.Item) error {
	if f.casHook != nil {
		f.casHook(item.Key)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return errFakeConn
	}
	it, ok := f.data[item.Key]
	if !ok {
		return memcache.ErrNotStored
	}
	if it.casID != item.CasID {
		return memcache.ErrCASConflict
	}
	f.nextID++
	it.value = append([]byte(nil), item.Value...)
	it.casID = f.nextID
	if item.Expiration != 0 {
		it.exptime = item.Expiration
	}
	return nil
}

func (f *fakeMemcache) Increment(key string, delta uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return 0, errFakeConn
	}
	it, ok := f.data[key]
	if !ok {
		return 0, memcache.ErrCacheMiss
	}
	cur, err := strconv.ParseUint(string(it.value), 10, 64)
	if err != nil {
		return 0, memcache.ErrServerError
	}
	cur += delta
	it.value = []byte(strconv.FormatUint(cur, 10))
	f.nextID++
	it.casID = f.nextID
	return cur, nil
}

func (f *fakeMemcache) Delete(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return errFakeConn
	}
	if _, ok := f.data[key]; !ok {
		return memcache.ErrCacheMiss
	}
	delete(f.data, key)
	return nil
}

func (f *fakeMemcache) Ping() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failConn {
		return errFakeConn
	}
	return nil
}

// newTestStore builds a Memcached store over a fresh fake client with the given
// options applied on top of test defaults.
func newTestStore(f *fakeMemcache) *Memcached {
	return newFromRawClient(f, Options{KeyPrefix: "t:"})
}
