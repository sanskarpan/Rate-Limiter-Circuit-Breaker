# GCRA vs token bucket: choosing a rate limiter

This library ships eight rate-limiting algorithms behind one `ratelimit.Limiter`
interface, but two of them cover the overwhelming majority of real API
rate-limiting needs: the **token bucket** and **GCRA** (the Generic Cell Rate
Algorithm). They admit traffic with the same *average* shape, yet differ in state
size, burst semantics, and how cleanly they distribute. This post walks through
both from the actual implementation in `ratelimit/tokenbucket` and `ratelimit/gcra`,
and ends on a precision subtlety that only appears once you run GCRA through a
Redis Lua script.

## Token bucket: the intuitive one

A bucket holds up to `capacity` tokens. Tokens refill at `refillRate` per second,
computed lazily — the code doesn't run a background ticker; it computes how many
tokens *would* have accrued since the last touch on demand. Each request consumes
one or more tokens; if the bucket can't cover the cost, the request is denied.

```go
limiter := tokenbucket.New(100, 20) // burst 100, 20 tokens/s sustained
```

The mental model is a leaky reservoir you draw from. The nice properties:

- **Burst is explicit and legible.** `capacity` *is* the burst you tolerate; a
  freshly idle key can spend all 100 tokens at once, then is throttled to the
  20/s refill.
- **Fractional cost is natural.** The bucket stores fractional tokens, so weighted
  or cost-based limiting ("this bulk write costs 5") drops in without extra
  machinery.

The cost is that the state is a small struct per key (token count plus a
last-refill timestamp), guarded by a map.

## GCRA: one timestamp does everything

GCRA encodes the entire state of a key as a **single timestamp** — the
Theoretical Arrival Time (TAT), the earliest moment the next request should be
allowed to arrive. The algorithm the library implements is exactly:

```
emissionInterval = window / limit
burstOffset      = emissionInterval × (burst - 1)
TAT              = max(lastTAT, now) + emissionInterval
allowed          = TAT - burstOffset ≤ now
retryAfter       = TAT - burstOffset - now   (when denied)
remaining        = floor((now + burstOffset - TAT) / emissionInterval)
```

```go
limiter := gcra.New(100, 10, time.Second) // 100/s, burst 10
```

What you get for that single timestamp:

- **Smooth, self-pacing output.** GCRA meters requests to one every
  `emissionInterval`, with `burstOffset` allowing a bounded head-start. There's no
  fixed-window boundary burst — the pathology where a client fires the full limit
  at the end of one window and again at the start of the next.
- **Minimal, distribution-friendly state.** One number per key is trivial to store
  and update atomically in Redis, which is why GCRA (and Stripe's and Shopify's
  well-known write-ups of it) is a favourite for distributed limiting.

The trade-off is that GCRA is less intuitive to reason about at a glance, and its
notion of "burst" is a time head-start rather than a token count.

## Which one to reach for

| You want… | Prefer |
|-----------|--------|
| An intuitive limiter for most API endpoints, with legible burst | Token bucket |
| Explicit, easy-to-explain burst capacity | Token bucket |
| Fractional / cost-weighted consumption with no extra plumbing | Token bucket |
| The smoothest output and no boundary-burst pathology | GCRA |
| The smallest per-key state for a distributed (Redis) limiter | GCRA |
| A strict "one request per interval" pacing guarantee | GCRA |

Both are available locally and as atomic Redis-backed distributed limiters, so
the choice is about semantics, not about what you can deploy.

## The precision subtlety GCRA exposes over Redis

Here's the part you only hit in production. When GCRA runs distributed, its TAT is
stored and compared inside a Redis **Lua script**, and Lua numbers are IEEE-754
**float64**. A TAT expressed in nanoseconds is a number around `1.78e18` — which
is *past* float64's `2^53` exact-integer ceiling (`~9.0e15`). Beyond that ceiling,
consecutive representable doubles are spaced more than 1 ns apart, so nanosecond
TATs snap to roughly **256-ns granularity** when Redis evaluates them.

The library treats this honestly rather than hand-waving it:

- The behaviour is **documented precisely** in the in-memory emulation
  (`ratelimit/store/scripts_memory.go`), which reproduces Redis's float64
  arithmetic exactly so distributed algorithms have a deterministic,
  dependency-free backend for tests — the emulation and real Redis agree to the
  bit (see [ADR-0003](../adr/0003-store-interface-and-lua-for-distributed.md)).
- It is promoted to a **documented accuracy guarantee**: a bounded ≤256-ns error
  that *never over-admits*, exported as `store.PrecisionBoundNs` and asserted by a
  dedicated precision test.

The lesson generalises: any limiter that stores absolute nanosecond timestamps and
does its arithmetic in a float64 environment (Lua, JavaScript, etc.) has this
ceiling. GCRA just makes it visible because its whole state *is* that timestamp.
Token bucket sidesteps it by storing a small token count and a delta, not an
ever-growing absolute time — one more axis on which the two algorithms differ once
you leave a single process.
