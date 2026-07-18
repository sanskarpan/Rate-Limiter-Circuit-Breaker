# Redis Key Lifecycle, TTL / GC Audit, and Hot-Key Considerations

_ENHANCEMENTS §5.3 — Key TTL/GC audit and hot-key protection._

This document audits the lifecycle of every Redis key created by the distributed
rate limiters and circuit breaker in
[`ratelimit/store/redis.go`](../ratelimit/store/redis.go), proves that **every
create path sets a TTL and every write path refreshes it** (so abandoned keys
self-reclaim), and records hot-key considerations. Line numbers below refer to
`ratelimit/store/redis.go` at the time of writing.

## Summary (audit result)

**No leak.** Every key any script can create is created together with a
`PEXPIRE`/`EXPIRE`/`SET ... PX`, and every subsequent write refreshes that TTL.
There is no code path that writes a key without an expiry. No additive `PEXPIRE`
was required — the guarantee already held; this document is the evidence.

The in-memory emulation ([`scripts_memory.go`](../ratelimit/store/scripts_memory.go))
mirrors this exactly: every handler calls `setEntryTTLmsAbs` on its write paths,
and the `*Memory` store additionally has a background janitor (`cleanup()`, every
`cleanupInterval`, default 30s), lazy expiry (`entry.isExpired`), and a `maxKeys`
guard (`reserveSlot`) that fails **closed** (denies new keys) when full.

## Per-script key lifecycle

| Script | Keys created | Create-path TTL | Refresh-on-write? | Notes |
|---|---|---|---|---|
| `TokenBucketScript` | 1 hash (`tokens`,`last_refill`) | `PEXPIRE ttl_ms` on **both** allow (L604) and deny (L608) | Yes — every call | `ttl_ms` = caller-computed full-refill time + margin; if absent the script derives `ceil(capacity/refill_rate)` (L495-497). |
| `FixedWindowScript` | 1 counter | `PEXPIRE ttl_ms` **only when INCRBY creates the key** (L636-638) | No (intentional) | TTL is pinned to the window's own start so the boundary is fixed; deny path (L629-631) does GET only — never creates a key. |
| `GCRAScript` | 1 TAT string | `SET ... PX ttl_ms` **only on allow** (L672) | Yes on allow | Deny path never writes → no key created on a rejected request. |
| `LeakyBucketScript` | 1 TAT string | `SET ... PX ttl_ms` **only on allow** (L731) | Yes on allow | Same as GCRA; deny never writes. |
| `SlidingWindowLogScript` | 1 ZSET | allow: `PEXPIRE ttl_ms` (L1048); deny: `EXPIRE ceil(ttl_ms/1000)` (L1042) | Yes — **both** allow and deny | See "ZSET growth bound" below. |
| `SlidingWindowCounterScript` | 2 counters (current, prev) | `PEXPIRE current_ttl_ms` **only when INCRBY creates current** (L781-783) | No (intentional) | Window-pinned TTL, like fixed window; prev key is created by a prior window's INCRBY and inherits that window's TTL. |
| `CircuitBreakerAcquireScript` | 1 hash | `PEXPIRE ttl_ms` on the half-open reserve path (L871) | Yes | Closed path returns without a write (no key created); the key first appears when a failure is recorded. |
| `CircuitBreakerRecordScript` | 1 hash | `PEXPIRE ttl_ms` **unconditionally at the end** (L967) | Yes — every record | Every outcome refreshes the TTL. |
| `CircuitBreakerReadScript` | none | — (read-only) | — | Only `HMGET`; never writes, so a read can neither create a key nor trip a transition. |
| `incrByScript` (Store.IncrBy) | 1 counter | `PEXPIRE ttl_ms` **only when INCRBY creates** (L288) | No (intentional) | Honors the Store contract "TTL only on creation" (fixes H-5/STORE-6 sliding-TTL bug). |

Non-scripted `Store` methods (`Set`, `SetNX`, `GetSet`) all take an explicit
`ttl time.Duration` and apply it atomically with the write; callers pass a
bounded TTL.

## ZSET growth bound (sliding-window-log)

The sliding-window-log ZSET is the only unbounded-in-principle structure, so it
gets special treatment:

1. **Bounded cardinality.** Every call begins with
   `ZREMRANGEBYSCORE key -inf (now-window)` (L1032), which evicts all members
   older than the window before counting. The live cardinality is therefore
   bounded by the number of admissions within one `window`, i.e. `≤ limit`
   (admission denies once `count + n > limit`). It cannot grow without bound.

2. **Empty-set auto-delete.** When the last member is pruned, Redis deletes the
   empty ZSET automatically, so an idle key disappears even before its TTL.

3. **TTL on every path.** Both the allow path (`PEXPIRE ttl_ms`, L1048) and the
   deny path (`EXPIRE ceil(ttl_ms/1000)`, L1042) refresh the key's TTL, so a key
   that stops being pruned (no further calls) still expires within `ttl_ms`. The
   caller sets `ttl_ms ≥ window` so a key that goes idle mid-window still lives
   long enough to serve its window, then self-reclaims.

## Fail-open / fallback interaction

When Redis is unreachable, every method routes to the fallback store
(`isConnectionError`, L184-206). The default fallback is a fresh in-memory store,
whose keys are governed by the same TTLs plus the janitor and `maxKeys` guard —
so GC continues to hold during an outage (at the documented cost of per-instance
divergence; see `RedisOptions.Fallback`).

## Hot-key considerations

- **Redis-side memory pressure.** The shipped `docker-compose.yml` configures
  `maxmemory-policy allkeys-lru`, so even a pathological key-churn workload cannot
  exhaust Redis memory — the LRU policy evicts cold keys. Combined with the
  per-key TTLs above, abandoned keys are reclaimed by (a) TTL expiry, (b) ZSET
  empty auto-delete, and (c) LRU eviction under pressure.
- **In-memory fallback bound.** The `*Memory` store's `WithMaxKeys` option caps
  the key count and fails **closed** (`errStoreFull` → deny) for a brand-new key
  once full, rather than admitting everything against a throwaway entry (F-4).
  This mirrors, on the fallback side, the Redis `allkeys-lru` protection.
- **Distributed max-key guard.** There is intentionally no distributed max-key
  cap in the Lua scripts: enforcing one would require an extra Redis structure
  (a global key-set/counter) on the hot path and a global lock, and the
  combination of per-key TTL + `allkeys-lru` already bounds Redis memory. Teams
  needing a hard cap should size `maxmemory` and rely on the eviction policy.

## Tests / evidence

- **TTL correctness under key churn** is covered by the store's fixes tests
  (`redis_fixes_test.go`, `memory_fixes_test.go`) — e.g. the H-5/STORE-6
  regression asserting fixed/sliding windows do **not** get a sliding TTL that
  never resets, and F-4 asserting the memory store fails closed at `maxKeys`.
- **Parity** between the Lua scripts and the in-memory emulation (including TTL
  and eviction behaviour) is asserted by the integration parity tests
  (`parity_integration_test.go`, `leakybucket_parity_integration_test.go`,
  run with `-tags=integration` against a live Redis).
- Run the store suite (no Redis needed) with:
  `go test -race ./ratelimit/store/...`
