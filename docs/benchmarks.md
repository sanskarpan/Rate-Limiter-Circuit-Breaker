# Benchmarks

Published, reproducible microbenchmarks for the hot-path primitives in this
library. **Every number below comes from a real `go test -bench` run on the
machine described in [Environment](#environment)** — nothing here is estimated
or hand-edited. Re-run them yourself with the commands in
[Reproducing](#reproducing) and you should land in the same order of magnitude
(absolute ns/op varies with CPU, thermal state, and Go version).

> These are *single-library* microbenchmarks (this library measured against
> itself across algorithms), not a head-to-head against other libraries. A
> cross-library comparison harness is tracked as future work in
> [ENHANCEMENTS.md §8.4](../ENHANCEMENTS.md). For a feature-level positioning
> against alternatives, see the comparison table in the
> [README](../README.md#how-this-compares-to-other-go-libraries).

## Environment

| Field | Value |
|-------|-------|
| Go version | `go1.26.4` |
| GOOS / GOARCH | `darwin` / `arm64` |
| CPU | Apple M3 Pro (11 cores) |
| `-count` | 3 |
| Benchmark flags | `-bench=. -benchmem -run='^$'` |

`GOMAXPROCS` was left at the default (number of logical CPUs), which is why the
benchmark names carry the `-11` suffix. These are laptop numbers on a shared,
non-quiesced machine — treat the ratios between algorithms as meaningful and the
absolute values as indicative rather than authoritative. CI runs a bounded
subset (token bucket, GCRA, circuit breaker) on GitHub-hosted runners as a
regression gate; those numbers will differ because the runner hardware differs.

## Results

Values are the **median of 3 runs** (`-count=3`), taken directly from the raw
output captured in the runs below. `Allow` on the local limiters is the common
single-decision hot path.

### Rate limiters — single-decision hot path

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `TokenBucket_Allow_SingleKey` | 187.7 | 0 | 0 |
| `TokenBucket_Allow_100Keys` | 184.4 | 0 | 0 |
| `GCRA_Allow_SingleKey` | 189.0 | 0 | 0 |
| `GCRA_Allow_100Keys` | 194.2 | 0 | 0 |
| `FixedWindow_Allow` | 187.3 | 0 | 0 |
| `SlidingWindowCounter_Allow` | 187.4 | 0 | 0 |
| `SlidingWindowCounter_Allow_100Keys` | 189.6 | 0 | 0 |
| `SlidingWindowLog_Allow` | 222.3 | 24 | 0 |
| `SlidingWindowLog_Allow_100Keys` | 233.2 | 2 | 0 |
| `LeakyBucket_Allow_SingleKey` | 4804 | 268 | 2 |
| `LeakyBucket_Allow_100Keys` | 65655 | 7722 | 2 |

Notes:

- **Token bucket, GCRA, fixed window, and sliding-window counter are all
  0-alloc, ~185–195 ns/op** for a single admission decision on this CPU.
- **Sliding-window log** carries a small per-op byte cost because it maintains a
  timestamp log per key; `allocs/op` still amortizes to 0 in these runs.
- **Leaky bucket is intentionally different**: it is a queue-and-drain shaper, so
  `Allow` may block on the drain interval. Its ns/op reflects that queuing
  behaviour, not per-decision overhead — do not compare it directly against the
  non-blocking limiters above. The `100Keys` figure is dominated by many
  concurrent short sleeps and is the noisiest number in the set.

### Rate limiters — parallel and multi-token paths

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `TokenBucket_Allow_Parallel` | 288.2 | 0 | 0 |
| `TokenBucket_AllowN_5` | 294.4 | 336 | 2 |
| `TokenBucket_Peek` | 274.0 | 360 | 5 |
| `GCRA_Allow_Parallel` | 285.5 | 0 | 0 |
| `FixedWindow_Allow_Parallel` | 266.1 | 0 | 0 |
| `SlidingWindowCounter_Allow_Parallel` | 286.1 | 0 | 0 |
| `SlidingWindowCounter_Peek` | 244.9 | 351 | 3 |
| `SlidingWindowLog_Allow_Parallel` | 356.8 | 6 | 0 |
| `LeakyBucket_Allow_Parallel` | 4049 | 264 | 2 |

Notes:

- The `Parallel` variants use `b.RunParallel`, so they include lock contention on
  the per-limiter key map. The single global `sync.RWMutex` per limiter is the
  contention point; sharding it is tracked in
  [ENHANCEMENTS.md §3.1](../ENHANCEMENTS.md).
- `AllowN` and `Peek` allocate because they populate `Result.Metadata`
  (a `map[string]any`). Making metadata lazy is tracked in
  [ENHANCEMENTS.md §3.2](../ENHANCEMENTS.md).

### Circuit breaker

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| `CB_Execute_Closed` | 107.7 | 0 | 0 |
| `CB_Execute_Open` | 146.1 | 48 | 1 |
| `CB_Execute_Closed_Parallel` | 222.3 | 0 | 0 |

Notes:

- The **closed** path (the common case: dependency healthy) is **0-alloc,
  ~108 ns/op**. It is a CAS-based counter update.
- The **open** path allocates one object: the `*CircuitError` returned to the
  caller when the breaker short-circuits.

## Coverage and gaps

Benchmarks exist today for the packages above. The following have **no
benchmarks yet** and are noted as future work rather than represented with
fabricated numbers:

- `bulkhead` — no benchmark file present.
- `ratelimit/adaptive`, `ratelimit/composite`, `ratelimit/tiered` — no benchmarks.
- `pipeline`, `retry`, `timeout`, `fallback` — no benchmarks.
- `ratelimit/middleware` (HTTP/gRPC) — no benchmarks.
- **Distributed / Redis paths** — no distributed benchmarks. Adding these
  (against `miniredis` for CI and real Redis for nightly) is tracked in
  [ENHANCEMENTS.md §3.3](../ENHANCEMENTS.md).

Adding these is a good first contribution — see
[docs/good-first-issues.md](good-first-issues.md).

## Methodology

- Each benchmark is run with `-count=3` and the reported value is the **median**
  of the three runs (chosen to reduce the influence of a single noisy sample).
- `-run='^$'` disables unit tests so only benchmarks execute.
- `-benchmem` reports `B/op` and `allocs/op` alongside `ns/op`.
- Numbers were **not** post-processed beyond selecting the median; the raw
  captured output is shown below for auditability.
- For statistically rigorous A/B comparisons (e.g. a PR vs `main`), use
  `benchstat` via `make bench-compare OLD=main NEW=HEAD` or
  `make bench-stat BASE=old.txt HEAD=new.txt`, which reports deltas with
  confidence intervals instead of raw medians.

## Reproducing

The exact command used to produce the rate-limiter and circuit-breaker tables:

```bash
go test -bench=. -benchmem -run='^$' -count=3 \
  ./ratelimit/tokenbucket/ \
  ./ratelimit/gcra/ \
  ./ratelimit/fixedwindow/ \
  ./ratelimit/slidingwindow/ \
  ./ratelimit/leakybucket/ \
  ./circuitbreaker/
```

Convenience Makefile targets:

```bash
make bench       # every benchmark in the repo, -benchmem -count=5, tee'd to bench-<version>.txt
make bench-ci    # the bounded CI regression set (token bucket, GCRA, circuit breaker)
```

Environment facts were captured with:

```bash
go version
go env GOOS GOARCH
sysctl -n machdep.cpu.brand_string   # macOS; on CI, note the GitHub runner class instead
```

## Raw output (excerpt)

Captured verbatim from the run described above (trimmed to representative lines;
`-count=3` means each benchmark appears three times in the full log):

```text
goos: darwin
goarch: arm64
pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket
cpu: Apple M3 Pro
BenchmarkTokenBucket_Allow_SingleKey-11    	 6746175	       182.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkTokenBucket_AllowN_5-11           	 4008825	       294.4 ns/op	     336 B/op	       2 allocs/op
BenchmarkTokenBucket_Allow_Parallel-11     	 4425452	       275.6 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra
BenchmarkGCRA_Allow_SingleKey-11    	 6470257	       189.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkGCRA_Allow_Parallel-11     	 4326399	       285.5 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow
BenchmarkFixedWindow_Allow-11             	 6346656	       187.3 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow
BenchmarkSlidingWindowLog_Allow-11                 	 5132568	       220.1 ns/op	      24 B/op	       0 allocs/op
BenchmarkSlidingWindowCounter_Allow-11             	 6505830	       187.4 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket
BenchmarkLeakyBucket_Allow_SingleKey-11    	  256716	      4614 ns/op	     269 B/op	       2 allocs/op

pkg: github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker
BenchmarkCB_Execute_Closed-11             	11452839	       107.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkCB_Execute_Open-11               	 7496500	       146.1 ns/op	      48 B/op	       1 allocs/op
```
