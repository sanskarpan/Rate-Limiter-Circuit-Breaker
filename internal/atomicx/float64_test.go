package atomicx_test

import (
	"math"
	"sync"
	"testing"

	"github.com/sanskarpan/resilience/internal/atomicx"
)

func TestFloat64_LoadStore(t *testing.T) {
	var a atomicx.Float64
	a.Store(3.14)
	if got := a.Load(); got != 3.14 {
		t.Fatalf("expected 3.14, got %v", got)
	}
}

func TestFloat64_Add(t *testing.T) {
	var a atomicx.Float64
	a.Store(1.0)
	result := a.Add(2.5)
	if result != 3.5 {
		t.Fatalf("expected 3.5, got %v", result)
	}
	if a.Load() != 3.5 {
		t.Fatalf("expected stored 3.5, got %v", a.Load())
	}
}

func TestFloat64_Sub(t *testing.T) {
	var a atomicx.Float64
	a.Store(10.0)
	result := a.Sub(3.0)
	if result != 7.0 {
		t.Fatalf("expected 7.0, got %v", result)
	}
}

func TestFloat64_CompareAndSwap_Success(t *testing.T) {
	var a atomicx.Float64
	a.Store(5.0)
	if !a.CompareAndSwap(5.0, 10.0) {
		t.Fatal("CAS should succeed when old matches")
	}
	if a.Load() != 10.0 {
		t.Fatalf("expected 10.0 after CAS, got %v", a.Load())
	}
}

func TestFloat64_CompareAndSwap_Failure(t *testing.T) {
	var a atomicx.Float64
	a.Store(5.0)
	if a.CompareAndSwap(6.0, 10.0) {
		t.Fatal("CAS should fail when old does not match")
	}
	if a.Load() != 5.0 {
		t.Fatalf("value should be unchanged, got %v", a.Load())
	}
}

func TestFloat64_Concurrent_Add(t *testing.T) {
	var a atomicx.Float64
	var wg sync.WaitGroup
	n := 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			a.Add(1.0)
		}()
	}
	wg.Wait()
	if got := a.Load(); got != float64(n) {
		t.Fatalf("expected %v, got %v", float64(n), got)
	}
}

func TestFloat64_ZeroValue(t *testing.T) {
	var a atomicx.Float64
	if a.Load() != 0.0 {
		t.Fatalf("zero value should load as 0.0, got %v", a.Load())
	}
}

// TestFloat64_Add_NormalAndNaN documents L-16 (ATOM-1): Add on a normal value
// works as expected, while CompareAndSwap comparisons involving NaN are
// bit-exact and therefore not reliable for equality (NaN != NaN in float terms,
// yet a bit-identical NaN CAS may succeed). See the doc comments on Float64.
func TestFloat64_Add_NormalAndNaN(t *testing.T) {
	var a atomicx.Float64
	a.Store(2.0)
	if got := a.Add(0.5); got != 2.5 {
		t.Fatalf("Add on normal value: expected 2.5, got %v", got)
	}

	// Storing NaN and CAS'ing against a freshly-produced NaN is undefined by
	// IEEE equality semantics; we only assert the documented bit-exact behavior:
	// a value that stores as NaN does not compare-equal to the numeric NaN in
	// the ordinary sense. Adding to a NaN state stays NaN.
	a.Store(math.NaN())
	if !math.IsNaN(a.Load()) {
		t.Fatalf("expected stored NaN, got %v", a.Load())
	}
	if got := a.Add(1.0); !math.IsNaN(got) {
		t.Fatalf("Add to NaN should remain NaN, got %v", got)
	}
}

func BenchmarkFloat64_Add(b *testing.B) {
	var a atomicx.Float64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.Add(1.0)
		}
	})
}
