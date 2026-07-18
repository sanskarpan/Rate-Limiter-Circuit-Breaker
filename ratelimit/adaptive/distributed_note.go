package adaptive

// Distributed adaptive limiting: intentionally node-local (ENHANCEMENTS §1.8).
//
// Unlike the leaky bucket, the adaptive limiter is deliberately NOT given a
// Redis/store-backed constructor, and this is a design decision rather than an
// omission.
//
// The adaptive limiter's whole job is to react to LOCAL system-health signals —
// this node's CPU utilisation, error rate, and P99 latency (see SignalSource in
// signals.go). Those signals are inherently per-instance: one node under GC
// pressure or serving a hot key can be stressed while its peers are idle. Sharing
// a single computed limit across a fleet would require either:
//
//   - a leader/aggregation model (elect a node, or aggregate every node's signals
//     into a global view and broadcast one limit), which adds a consensus/gossip
//     dependency and a decision-latency hop on a path whose entire value is fast
//     local reaction; or
//   - each node writing its computed limit to the store and reading back some
//     reduction (min/mean), which produces a limit that reflects no single node's
//     real state and can oscillate as nodes disagree.
//
// Both contradict the pattern's purpose and its zero-dependency guarantee. The
// recommended distributed composition is therefore:
//
//   - Use a DISTRIBUTED rate limiter (e.g. leakybucket.NewDistributed,
//     tokenbucket.NewDistributed, or gcra.NewDistributed) for the shared,
//     fleet-wide ceiling that must hold globally, and
//   - Use a node-local AdaptiveLimiter in front of it as a per-instance
//     back-pressure valve that sheds load early when THIS node is unhealthy.
//
// This keeps the global invariant in the store (where it belongs) and the fast
// local health reaction in-process (where it belongs), without a leader election
// or an approximate shared-limit protocol.
