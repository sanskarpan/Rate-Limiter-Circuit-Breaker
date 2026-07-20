package shardmap

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNextPow2(t *testing.T) {
	cases := map[int]int{-1: 1, 0: 1, 1: 1, 2: 2, 3: 4, 5: 8, 8: 8, 9: 16, 17: 32}
	for in, want := range cases {
		if got := nextPow2(in); got != want {
			t.Errorf("nextPow2(%d)=%d want %d", in, got, want)
		}
	}
}

func TestShardsPowerOfTwo(t *testing.T) {
	m := New[int]()
	n := m.Shards()
	if n < 1 || n&(n-1) != 0 {
		t.Fatalf("shard count %d is not a power of two >= 1", n)
	}
}

func TestGetOrCreateSingleCreation(t *testing.T) {
	m := New[int]()
	var creates atomic.Int64
	const goroutines = 64
	var wg sync.WaitGroup
	results := make([]*int, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = m.GetOrCreate("same-key", func() *int {
				creates.Add(1)
				v := 42
				return &v
			})
		}(i)
	}
	wg.Wait()
	if creates.Load() != 1 {
		t.Fatalf("create called %d times, want exactly 1", creates.Load())
	}
	// All goroutines must observe the identical pointer.
	first := results[0]
	for i, r := range results {
		if r != first {
			t.Fatalf("goroutine %d got a different pointer", i)
		}
	}
}

func TestGetDeleteRange(t *testing.T) {
	m := New[int]()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k-%d", i)
		v := i
		got := m.GetOrCreate(k, func() *int { return &v })
		if *got != i {
			t.Fatalf("GetOrCreate(%s)=%d want %d", k, *got, i)
		}
	}
	if m.Len() != 1000 {
		t.Fatalf("Len=%d want 1000", m.Len())
	}
	if v, ok := m.Get("k-500"); !ok || *v != 500 {
		t.Fatalf("Get(k-500)=%v,%v", v, ok)
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("Get(missing) returned ok=true")
	}

	// Range visits every entry exactly once.
	seen := make(map[string]bool)
	m.Range(func(k string, _ *int) bool {
		if seen[k] {
			t.Fatalf("Range visited %s twice", k)
		}
		seen[k] = true
		return true
	})
	if len(seen) != 1000 {
		t.Fatalf("Range saw %d entries want 1000", len(seen))
	}

	m.Delete("k-500")
	if _, ok := m.Get("k-500"); ok {
		t.Fatal("k-500 still present after Delete")
	}
	if m.Len() != 999 {
		t.Fatalf("Len=%d want 999 after delete", m.Len())
	}
}

func TestRangeEarlyStop(t *testing.T) {
	m := New[int]()
	for i := 0; i < 100; i++ {
		v := i
		m.GetOrCreate(fmt.Sprintf("k-%d", i), func() *int { return &v })
	}
	count := 0
	m.Range(func(string, *int) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Fatalf("Range visited %d entries, want early stop at 10", count)
	}
}

func TestDeleteMatching(t *testing.T) {
	m := New[int]()
	for i := 0; i < 100; i++ {
		v := i
		m.GetOrCreate(fmt.Sprintf("k-%d", i), func() *int { return &v })
	}
	m.DeleteMatching(func(_ string, v *int) bool { return *v%2 == 0 })
	if m.Len() != 50 {
		t.Fatalf("Len=%d want 50 after deleting evens", m.Len())
	}
	m.Range(func(_ string, v *int) bool {
		if *v%2 == 0 {
			t.Fatalf("even value %d survived DeleteMatching", *v)
		}
		return true
	})
}

// TestConcurrentMixed exercises Get/GetOrCreate/Delete/Range under the race
// detector to catch any missing synchronization.
func TestConcurrentMixed(t *testing.T) {
	m := New[int]()
	const workers = 32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k-%d", (w*7+i)%64)
				v := i
				m.GetOrCreate(k, func() *int { return &v })
				m.Get(k)
				if i%17 == 0 {
					m.Delete(k)
				}
				if i%53 == 0 {
					m.Range(func(string, *int) bool { return true })
				}
			}
		}(w)
	}
	wg.Wait()
}
