# Algorithm Deep-Dives

> Part of the [Resilience](../README.md) documentation.
> See also: [Algorithm comparison](comparison.md) · [Distributed rate limiting](distributed.md)

Each algorithm is identified in the demo server and HTTP API by a canonical
underscore name, shown in parentheses below.

## Token Bucket (`token_bucket`)

**Theory:** A bucket holds up to `capacity` tokens. Tokens refill at `refillRate` per second (lazy — computed on demand). Each request consumes 1+ tokens. If the bucket is empty, the request is denied.

**Mathematical Model:**
```
tokens(t) = min(capacity, tokens(t₀) + (t - t₀) * refillRate)
```

**Properties:**
- Burst up to `capacity` tokens
- Sustained rate = `refillRate` req/s
- O(1) time, O(keys) memory
- `AllowN(n)` is fully atomic — never partial consumption

**When to use:** Most API rate limiting scenarios. Allows burst for legitimate traffic spikes.

---

## Leaky Bucket (`leaky_bucket`)

**Theory:** Requests enter a FIFO queue ("bucket") of size `capacity`. A background goroutine drains ("leaks") requests at exactly `leakRate` req/s.

**Output guarantee:**
```
output_rate ≤ leakRate (strictly)
input_rate = any (up to capacity; excess dropped)
```

**Properties:**
- No burst — strictly constant output
- Smooths bursty input
- Adds queuing latency: up to `capacity / leakRate` seconds
- O(keys + queue_depth) memory

**When to use:** Outgoing calls to partner APIs with strict SLAs. Never use for user-facing latency-sensitive paths.

---

## Sliding Window — Log variant (`sliding_window`)

**Theory:** Maintain a sorted list of request timestamps for each key. On each request:
1. Remove timestamps older than `now - window`
2. Count remaining — if ≥ limit, deny
3. Append `now` and allow

**RetryAfter formula:**
```
retryAfter = oldest_timestamp_in_window + window - now
```

**Properties:**
- Exact counting (no approximation)
- Memory: O(requests_per_window_per_key) — grows with load
- Eliminates boundary burst of Fixed Window

---

## Sliding Window — Counter variant (`sliding_window`)

**Theory:** Two counters: `current` (this window) + `previous` (last window). Compute effective rate with weighted formula:

```
effectiveCount = previous.count × (1 - elapsed/window) + current.count
```

**Approximation error:** max `limit × (1/windowBuckets)` — typically <1% error.

**Properties:**
- O(keys) memory — constant regardless of request rate
- ~1% approximation error at window boundary
- No boundary burst

---

## Fixed Window Counter (`fixed_window`)

**Theory:** Divide time into fixed windows. Count requests in the current window. Reset at window boundary.

**Boundary Burst Problem:**
```
Window N ends, Window N+1 starts
→ 2× limit requests possible in 2× window duration at boundary
```

**Properties:**
- Simplest algorithm
- Fastest implementation
- Known boundary burst vulnerability

**When to use:** Internal rate limits where boundary burst is acceptable. Never for security-critical limits.

---

## GCRA — Generic Cell Rate Algorithm (`gcra`)

**Theory:** One timestamp (Theoretical Arrival Time, TAT) per key encodes the full state.

**Core formula:**
```
emissionInterval = window / limit
burstOffset      = emissionInterval × (burst - 1)
TAT              = max(lastTAT, now) + emissionInterval
allowed          = TAT - burstOffset ≤ now
retryAfter       = TAT - burstOffset - now  (when denied)
remaining        = floor((now + burstOffset - TAT) / emissionInterval)
```

**Properties:**
- One timestamp per key (minimal memory)
- Mathematically exact
- Redis-optimal: single GET+SET CAS loop
- No floating point — all `time.Duration` (int64 ns)

**References:**
- ATM Forum Traffic Management specification
- Brandur Leach: "Rate Limiting with Redis"
- RFC 2697 (Single Rate Three Color Marker)

---

## Adaptive (`adaptive`)

**Theory:** Not a standalone counting algorithm — the adaptive limiter tracks a
*limit* that it retunes at runtime from live signals (observed latency and
error rate) between a configured `minLimit` and `maxLimit`. When downstream
health degrades it shrinks the limit (load shedding); when health recovers it
grows it back.

**Properties:**
- Dynamic limit within `[minLimit, maxLimit]`
- O(keys) memory
- Local only — there is no distributed variant, since the tuning decision is
  based on each instance's own observed signals

**When to use:** Protecting a downstream whose safe throughput varies with its
own load — the limiter backs off automatically instead of using a fixed ceiling.

---

See also: [Algorithm comparison](comparison.md) · [Distributed rate limiting](distributed.md) · [README](../README.md)
