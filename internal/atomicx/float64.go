// Package atomicx provides atomic operations for types not covered by sync/atomic.
// All types and methods are safe for concurrent use.
package atomicx

import (
	"math"
	"sync/atomic"
)

// Float64 provides atomic load/store/add operations for float64 values.
// Uses math.Float64bits / math.Float64frombits with sync/atomic underneath.
// This avoids a mutex on the hot path for token count in TokenBucket.
//
// NaN caveat (ATOM-1): all comparisons performed by CompareAndSwap are
// bit-exact on the underlying uint64 representation, not IEEE-754 float
// equality. Consequently NaN handling is undefined/surprising:
//   - CompareAndSwap(NaN, x) may succeed if the stored bits happen to match
//     the exact NaN bit pattern passed in, even though NaN != NaN by IEEE
//     rules; and it will fail across differing NaN payloads that are
//     "equal" in the float sense.
//   - Add/Sub on a NaN value propagate NaN as usual.
//
// Callers must not rely on CompareAndSwap for NaN values. Store normal
// (non-NaN) values on the CAS hot paths.
type Float64 struct {
	v uint64
}

// Load atomically loads and returns the float64 value.
func (a *Float64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&a.v))
}

// Store atomically stores the float64 value.
func (a *Float64) Store(f float64) {
	atomic.StoreUint64(&a.v, math.Float64bits(f))
}

// Add atomically adds delta to the value and returns the new value.
func (a *Float64) Add(delta float64) float64 {
	for {
		old := atomic.LoadUint64(&a.v)
		oldF := math.Float64frombits(old)
		newF := oldF + delta
		newBits := math.Float64bits(newF)
		if atomic.CompareAndSwapUint64(&a.v, old, newBits) {
			return newF
		}
	}
}

// CompareAndSwap atomically compares the current value with old and,
// if they are equal (using bit-exact comparison), sets it to new.
// Returns true if the swap was performed.
//
// The comparison is bit-exact on the float64's uint64 representation, so it
// is NaN-fragile: NaN comparisons do not follow IEEE-754 equality (see the
// Float64 type doc). Do not use CompareAndSwap with NaN operands.
func (a *Float64) CompareAndSwap(old, new float64) bool {
	return atomic.CompareAndSwapUint64(&a.v, math.Float64bits(old), math.Float64bits(new))
}

// Sub atomically subtracts delta from the value and returns the new value.
func (a *Float64) Sub(delta float64) float64 {
	return a.Add(-delta)
}
