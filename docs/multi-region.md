# Multi-Region / Active-Active Strategy

This is an **honest strategy note**, not a feature promise. Today the library's
distributed backend (`ratelimit/store`, Redis) targets a **single region with a
shared Redis**. This page explains what that means for multi-region deployments,
what the options are, and where future work (a CRDT/gossip counter) would fit.

## What the library provides today

- A single distributed backend: Redis, behind the `store.Store` interface
  ([ADR-0003](adr/0003-store-interface-and-lua-for-distributed.md)).
- Per-key atomic decisions via Lua, exact while Redis is reachable, **fail-open**
  when it is not ([ADR-0004](adr/0004-fail-open-resilience-philosophy.md)).
- Optional `UseServerTime` to pin all instances to the Redis server clock, which
  matters more as instances spread across hosts/regions with skewed clocks.

The implicit assumption is that all instances sharing a limit talk to the **same**
Redis. That holds cleanly **within** a region. Across regions it forces a design
choice, because a single Redis cannot be close to every region at once.

## The core tension

Global limits across regions require choosing between two properties you cannot
maximize simultaneously:

- **Accuracy** — one authoritative counter, so the global limit is exact.
- **Latency** — every rate-limit check is on the request hot path, so a
  cross-region round-trip to a distant authoritative store adds tens to hundreds
  of milliseconds to *every* request.

## Recommended default: region-local limiting

For most systems, **limit region-locally** and accept that the global limit is
approximately `per_region_limit × regions`:

- Each region runs its own Redis (or in-memory limiters) and enforces a
  region-local budget. No request pays a cross-region hop for a rate-limit check.
- Latency stays low and predictable; a region outage does not take down limiting
  in other regions.
- Set each region's budget to its share of the global target (e.g. by traffic
  split). This is coarse but robust, and it is what the library supports well
  today with a per-region shared Redis.

Region-local limiting is the pragmatic default because rate limiting is a hot-path
concern where the cost of a wrong cross-region round-trip usually outweighs the
benefit of a perfectly exact global count.

## Option: a single shared global Redis

Point every region at **one** authoritative Redis (or a Redis primary in one
region):

- **Pro:** the global limit is exact — one counter, one clock (pair it with
  `UseServerTime`).
- **Con:** every check from a remote region pays the inter-region latency, on the
  hot path, for every request. And that Redis becomes a **cross-region single
  point of failure**: when a remote region can't reach it, this library's
  **fail-open** behaviour degrades to per-instance local limiting in that region
  ([ADR-0004](adr/0004-fail-open-resilience-philosophy.md)) — availability is
  preserved, exactness is not.

This is viable only when regions are latency-close and you genuinely need an exact
global limit more than you need low, independent per-region latency.

## Option: per-region Redis with reconciliation

Run a Redis per region for hot-path decisions, and reconcile budgets out of band
(e.g. a control loop that periodically redistributes the global budget across
regions based on observed usage). This keeps checks region-local (low latency)
while steering the *aggregate* toward the global target. It trades exactness for a
good approximation without a hot-path cross-region hop. The library does not ship
this reconciler; it is an application-level pattern layered on per-region stores.

## Where a CRDT / gossip counter would fit

For true active-active global limiting **without** a hot-path cross-region
round-trip, the natural fit is an **approximate, eventually-consistent counter**:

- A **G-Counter** (grow-only CRDT): each region maintains its own increment-only
  sub-counter and gossips it; the global count is the sum. Merges are commutative
  and conflict-free, so regions converge without coordination.
- A **gossip-based** approximate limiter (e.g. SWIM/memberlist-style membership
  with periodic state exchange) for a no-external-dependency, in-cluster variant.

Both are **eventually consistent** and therefore **over-admit** by a bounded
amount during propagation delay: while region A's recent increments haven't yet
reached region B, both may still admit. The right way to ship this would be behind
the existing `store.Store` interface with a **documented over-admission bound**,
so callers can reason about the worst case. This is sketched in ENHANCEMENTS §5.6
as a design direction, not a committed feature.

## Summary

- **Today:** single-region shared Redis; region-local limiting is the recommended
  multi-region approach.
- **Shared global Redis:** exact but pays cross-region latency on every check and
  becomes a cross-region SPOF (fail-open on partition).
- **Per-region Redis + reconciliation:** low latency, approximate global budget,
  application-level pattern.
- **CRDT / gossip counter:** the right long-term fit for active-active, eventually
  consistent with a bounded over-admission — a future direction, not shipped.
