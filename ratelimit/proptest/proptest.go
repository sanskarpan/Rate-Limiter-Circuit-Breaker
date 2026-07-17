// Package proptest holds property-based tests (using pgregory.net/rapid) that
// assert algebraic invariants over the concrete rate limiters under any random
// schedule of operations and deterministic clock advances.
//
// It is an EXTERNAL test package: it depends only on the public limiter APIs,
// internal/clock.ManualClock, and rapid. It contains no production code — the
// single exported symbol here is the package doc.
//
// The properties asserted are (see ENHANCEMENTS.md §6.1):
//
//  1. Admission bound — over any random Allow/AllowN + advance schedule, the
//     number of admitted tokens within the relevant accounting window never
//     exceeds the algorithm-specific ceiling (rate × elapsed + burst).
//  2. Remaining invariant — Result.Remaining ∈ [0, limit] for every decision.
//  3. Determinism — replaying an identical op schedule on a fresh limiter +
//     ManualClock yields byte-identical decisions.
//  4. AllowN atomicity — AllowN(n) is all-or-nothing; a denied AllowN never
//     partially consumes, verified via Peek before/after.
//  5. Reset — after Reset a fresh full burst is admittable again.
package proptest
