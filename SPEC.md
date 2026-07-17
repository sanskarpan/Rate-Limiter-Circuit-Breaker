# Claude Code Prompt — Production Rate Limiter + Circuit Breaker Library (Go + Next.js)

> **Companion file:** This prompt has a `tickets.md` file with 36 phase-wise tickets for step-by-step execution. When using with Claude Code, feed one phase at a time from `tickets.md`. Use this prompt as the authoritative specification — `tickets.md` is the execution plan.

---

## PROJECT OVERVIEW

Build a **production-grade, publishable Go library** implementing every major rate limiting algorithm and resilience pattern from scratch — along with a **Next.js visual playground** that lets you interact with, benchmark, and understand every algorithm in real time.

This is two things simultaneously:
1. **A real Go library** (`github.com/sanskarpan/resilience`) — importable, well-documented, benchmark-tested, usable in production HTTP servers, gRPC services, and any Go application
2. **An educational + operational dashboard** — built in Next.js — that visualizes algorithm internals, simulates load, and exposes real-time behavior

**Stack:**
- **Library:** Go (Golang) — pure stdlib, zero external runtime dependencies, optional Redis adapter as a separate package
- **Demo server:** Go — HTTP server exposing the library via REST + WebSocket + gRPC endpoints for the frontend to hit
- **Frontend:** Next.js 14+ (App Router) + TypeScript + Tailwind CSS + shadcn/ui — real-time algorithm visualizer, load simulator, circuit breaker state machine viewer

**Philosophy:** Every algorithm is implemented from first principles. No `golang.org/x/time/rate` for the core logic. No third-party circuit breaker packages. You will understand exactly how every byte of state is managed.

---

## REPOSITORY STRUCTURE

```
resilience/
├── ratelimit/                          # Rate limiting package (importable library)
│   ├── tokenbucket/
│   │   ├── tokenbucket.go             # Core token bucket implementation
│   │   ├── tokenbucket_test.go        # Unit + race tests
│   │   ├── tokenbucket_bench_test.go  # Benchmarks
│   │   └── distributed.go             # Redis-backed distributed token bucket
│   ├── leakybucket/
│   │   ├── leakybucket.go             # Leaky bucket (queue-based)
│   │   ├── leakybucket_test.go
│   │   └── leakybucket_bench_test.go
│   ├── slidingwindow/
│   │   ├── log.go                     # Sliding window log (exact, per-request timestamps)
│   │   ├── counter.go                 # Sliding window counter (approximated, memory efficient)
│   │   ├── slidingwindow_test.go
│   │   └── distributed.go             # Redis sorted set implementation
│   ├── fixedwindow/
│   │   ├── fixedwindow.go             # Fixed window counter
│   │   ├── fixedwindow_test.go
│   │   └── distributed.go             # Redis-backed with atomic INCR + EXPIRE
│   ├── gcra/
│   │   ├── gcra.go                    # Generic Cell Rate Algorithm (IETF leaky bucket variant)
│   │   ├── gcra_test.go
│   │   └── distributed.go             # Redis GCRA (single key, CAS loop)
│   ├── adaptive/
│   │   ├── adaptive.go                # Adaptive rate limiter (adjusts limit based on system load)
│   │   └── adaptive_test.go
│   ├── composite/
│   │   ├── composite.go               # Chain multiple limiters (AND/OR semantics)
│   │   └── composite_test.go
│   ├── middleware/
│   │   ├── http.go                    # net/http middleware (works with chi, gin, echo, stdlib)
│   │   ├── grpc.go                    # gRPC unary + streaming interceptors
│   │   ├── fiber.go                   # Fiber framework adapter
│   │   └── options.go                 # Key extraction: IP, user ID, API key, custom
│   ├── store/
│   │   ├── store.go                   # Store interface (Get, Set, IncrBy, SetNX, Expire)
│   │   ├── memory.go                  # In-memory store (sync.Map + atomic)
│   │   └── redis.go                   # Redis store (go-redis/v9)
│   └── limiter.go                     # Limiter interface + Result type
│
├── circuitbreaker/                     # Circuit breaker package
│   ├── circuitbreaker.go              # Core circuit breaker (Closed/Open/HalfOpen FSM)
│   ├── circuitbreaker_test.go
│   ├── circuitbreaker_bench_test.go
│   ├── config.go                      # Config, thresholds, timeouts, callbacks
│   ├── state.go                       # State machine + transition logic
│   ├── metrics.go                     # Internal window-based failure tracking
│   ├── registry.go                    # Global registry — manage named circuit breakers
│   └── middleware/
│       ├── http.go                    # net/http middleware wrapper
│       └── grpc.go                    # gRPC interceptor
│
├── bulkhead/                           # Bulkhead / concurrency limiter
│   ├── bulkhead.go                    # Semaphore-based concurrency limiter
│   ├── threadpool.go                  # Fixed thread pool bulkhead
│   └── bulkhead_test.go
│
├── retry/                              # Retry package
│   ├── retry.go                       # Core retry with policy
│   ├── backoff/
│   │   ├── constant.go                # Constant delay
│   │   ├── exponential.go             # Exponential backoff
│   │   ├── jitter.go                  # Full jitter + equal jitter (AWS paper)
│   │   └── decorrelated.go            # Decorrelated jitter (best for distributed)
│   └── retry_test.go
│
├── timeout/                            # Timeout / deadline package
│   ├── timeout.go                     # Context-based timeout wrapper
│   └── timeout_test.go
│
├── fallback/                           # Fallback / hedge request package
│   ├── fallback.go                    # Execute primary, fall back on error/timeout
│   ├── hedge.go                       # Hedge requests: fire backup after P99 latency
│   └── fallback_test.go
│
├── pipeline/                           # Combine patterns into a resilience pipeline
│   ├── pipeline.go                    # Builder: ratelimit → bulkhead → timeout → CB → retry
│   └── pipeline_test.go
│
├── server/                             # Demo HTTP + WebSocket + gRPC server
│   ├── main.go
│   ├── api/
│   │   ├── router.go
│   │   ├── ratelimit_handlers.go      # Endpoints to exercise each limiter
│   │   ├── circuitbreaker_handlers.go # Endpoints to control CB state
│   │   ├── benchmark_handlers.go      # Load generation + metrics endpoints
│   │   ├── websocket.go               # Real-time state streaming
│   │   └── middleware.go
│   └── proto/
│       └── demo.proto                 # gRPC service definition
│
├── frontend/                           # Next.js application
│
├── docs/
│   ├── algorithms.md                  # Algorithm deep-dives with math
│   ├── comparison.md                  # When to use which algorithm
│   └── distributed.md                 # Distributed rate limiting patterns
│
├── examples/
│   ├── http-server/                   # Example: rate-limited HTTP server
│   ├── grpc-server/                   # Example: rate-limited gRPC server
│   ├── distributed/                   # Example: Redis-backed distributed limiting
│   └── pipeline/                      # Example: full resilience pipeline
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── release.yml
├── go.mod
├── go.sum
├── Makefile
├── Dockerfile
└── docker-compose.yml
```

---

## LIBRARY SPECIFICATIONS — RATE LIMITERS

### Core Interface

```go
// Package ratelimit defines the core interfaces for all rate limiting implementations.
package ratelimit

// Limiter is the core interface all rate limiters implement.
type Limiter interface {
    // Allow checks if a single token is available. Non-blocking.
    Allow(ctx context.Context, key string) Result

    // AllowN checks if n tokens are available. Non-blocking.
    AllowN(ctx context.Context, key string, n int) Result

    // Wait blocks until a token is available or ctx is cancelled.
    Wait(ctx context.Context, key string) error

    // WaitN blocks until n tokens are available or ctx is cancelled.
    WaitN(ctx context.Context, key string, n int) error

    // Peek returns current state without consuming a token.
    Peek(ctx context.Context, key string) State

    // Reset resets all state for a given key.
    Reset(ctx context.Context, key string) error

    // Close releases all resources.
    Close() error
}

// Result is returned by Allow/AllowN.
type Result struct {
    Allowed    bool          // Was the request permitted?
    Limit      int           // Configured limit
    Remaining  int           // Tokens remaining after this request
    ResetAfter time.Duration // When the window/bucket resets
    RetryAfter time.Duration // How long to wait before retrying (if not allowed)
    Metadata   map[string]any // Algorithm-specific metadata for observability
}

// State is returned by Peek — current limiter state without side effects.
type State struct {
    Key        string
    Limit      int
    Remaining  int
    ResetAt    time.Time
    WindowStart time.Time
    Algorithm  string
    Extra      map[string]any // Algorithm-specific: token count, queue depth, etc.
}
```

---

### Algorithm 1: Token Bucket

**Theory:** A bucket holds up to `capacity` tokens. Tokens are added at rate `refillRate` tokens/second. Each request consumes one or more tokens. If the bucket is empty, the request is denied (or queued).

**Implementation requirements (`ratelimit/tokenbucket/tokenbucket.go`):**

```go
type TokenBucket struct {
    capacity   float64       // Maximum tokens
    refillRate float64       // Tokens added per second
    tokens     float64       // Current token count (float for fractional accumulation)
    lastRefill time.Time     // When tokens were last added
    mu         sync.Mutex
    clock      Clock         // Mockable clock interface for testing
}
```

- Token accumulation: on every `Allow` call, compute elapsed time since `lastRefill`, calculate `elapsed.Seconds() * refillRate` tokens to add, cap at `capacity`. This is lazy refill — no background goroutine needed.
- `AllowN(n)`: consume n tokens atomically — succeed or fail atomically, never partial consume
- `Wait(ctx)`: if insufficient tokens, compute exact wait duration `= (n - tokens) / refillRate`, sleep using a timer, re-check (tokens may have arrived)
- **Burst handling**: capacity > refillRate allows bursting — document this clearly
- **Clock interface**: all time operations go through a `Clock` interface:
  ```go
  type Clock interface {
      Now() time.Time
      Sleep(d time.Duration)
      NewTimer(d time.Duration) *time.Timer
  }
  ```
  Real clock uses `time.Now()`. Test clock is manually advanceable — critical for deterministic tests.
- **Atomic fast path**: use `sync/atomic` for the common `Allow(1)` case (no contention) — only acquire mutex for refill calculation
- **Metadata in Result**: include `tokens_before`, `tokens_after`, `refilled_amount`, `time_to_full`

**Distributed Token Bucket** (`tokenbucket/distributed.go`):
- Use Redis with a Lua script for atomic check-and-set (no race condition between check and decrement)
- Lua script: load current tokens + last refill time, compute refill, check if sufficient, decrement, store back — all in one atomic script execution
- Key expiry: set Redis key TTL to `capacity / refillRate * 2` — auto-cleanup for inactive keys
- Handle Redis unavailability: configurable fallback (allow all, deny all, or use local bucket)

---

### Algorithm 2: Leaky Bucket

**Theory:** Requests enter a queue (the "bucket"). Requests are processed (leaked) at a constant rate. If the queue is full, new requests are dropped. This smooths bursty traffic into a constant output rate.

**Implementation requirements (`ratelimit/leakybucket/leakybucket.go`):**

```go
type LeakyBucket struct {
    rate     float64       // Requests processed per second (leak rate)
    capacity int           // Maximum queue depth
    queue    chan struct{}  // Buffered channel as the queue
    lastLeak time.Time
    mu       sync.Mutex
    clock    Clock
    done     chan struct{}  // shutdown signal
    wg       sync.WaitGroup
}
```

- **Queue-based**: use a buffered channel of size `capacity` as the queue
- **Background leaker goroutine**: leaks one request every `1/rate` seconds using `time.Ticker`. Goroutine exits on `Close()`
- `Allow()`: try to send to channel (non-blocking with `select default`). If channel full → denied.
- `Wait()`: send to channel (blocking). Respect context cancellation.
- **Queue depth in Peek()**: `len(queue)` gives current queue depth — expose as `State.Extra["queue_depth"]`
- **Important distinction from token bucket**: leaky bucket enforces constant output rate regardless of input burst. Token bucket allows bursting up to capacity. Document this difference prominently in godoc.
- `Close()`: drain channel, stop ticker goroutine, wait for WaitGroup

---

### Algorithm 3: Sliding Window Log

**Theory:** Maintain a log of timestamps of all requests within the window. On each request, remove timestamps older than `window`, count remaining — if count < limit, allow and append. Exact but memory-intensive: O(n) where n = requests per window.

**Implementation requirements (`ratelimit/slidingwindow/log.go`):**

```go
type SlidingWindowLog struct {
    limit  int
    window time.Duration
    log    map[string][]time.Time  // per-key request timestamps
    mu     sync.RWMutex
    clock  Clock
}
```

- On `Allow(key)`:
  1. Acquire write lock
  2. Remove all timestamps < `now - window` from `log[key]`
  3. If `len(log[key]) >= limit` → denied (RetryAfter = oldest entry + window - now)
  4. Append `now` to `log[key]`
  5. Return allowed
- **Memory cleanup**: background goroutine evicts keys with no timestamps in the last `window * 2` — runs every `window` duration
- **Memory metadata**: expose `log_size` (number of stored timestamps for this key) in `Result.Metadata`
- **Distributed** (`slidingwindow/distributed.go`): use Redis sorted set (ZSET) — score = Unix timestamp in milliseconds, member = unique request ID (UUID). Commands:
  ```
  MULTI
  ZREMRANGEBYSCORE key 0 (now_ms - window_ms)
  ZCARD key
  ZADD key now_ms <uuid>
  EXPIRE key window_seconds
  EXEC
  ```
  Parse ZCARD result to determine allow/deny. Use pipeline for atomicity.

---

### Algorithm 4: Sliding Window Counter

**Theory:** Approximate the sliding window using two fixed windows (current + previous). Weight the previous window's count by the fraction of the window that has elapsed. Highly memory efficient: only 2 counters per key instead of N timestamps.

**Formula:** `effective_count = previous_count * (1 - elapsed/window) + current_count`

If `effective_count >= limit` → deny.

**Implementation requirements (`ratelimit/slidingwindow/counter.go`):**

```go
type SlidingWindowCounter struct {
    limit         int
    window        time.Duration
    current       map[string]windowBucket
    previous      map[string]windowBucket
    mu            sync.RWMutex
    clock         Clock
}

type windowBucket struct {
    count     int
    windowStart time.Time
}
```

- On each request:
  1. Determine current window start: `floor(now / window) * window`
  2. If `current[key].windowStart` has advanced, shift: `previous[key] = current[key]`, reset `current[key]`
  3. Compute `elapsed = now - currentWindowStart`
  4. `effectiveCount = float64(previous[key].count) * float64(window - elapsed) / float64(window) + float64(current[key].count)`
  5. If `effectiveCount >= limit` → deny
  6. Increment `current[key].count`
- **Expose approximation error** in Metadata: `approximation_method: "sliding_window_counter"`, `effective_count`, `previous_count`, `current_count`, `elapsed_fraction`
- **Distributed** (`slidingwindow/distributed.go`): Redis INCR + EXPIRE on two keys (`{key}:current:{window_epoch}` and `{key}:previous:{window_epoch}`), use Lua script for atomic read-both-increment-one

---

### Algorithm 5: Fixed Window Counter

**Theory:** Divide time into fixed windows (e.g., every 60 seconds). Count requests per window. Reset counter at window boundary. Simple and fast but has a "boundary burst" problem — 2x limit requests possible straddling a window boundary.

**Implementation requirements (`ratelimit/fixedwindow/fixedwindow.go`):**

```go
type FixedWindowCounter struct {
    limit      int
    window     time.Duration
    counters   map[string]counter
    mu         sync.RWMutex
    clock      Clock
}

type counter struct {
    count       int
    windowStart time.Time
}
```

- On each request:
  1. Compute window start: `floor(now.UnixNano() / window.Nanoseconds()) * window.Nanoseconds()`
  2. If stored `windowStart` differs → reset counter for this key
  3. If `count >= limit` → deny with `RetryAfter = windowStart + window - now`
  4. Increment and allow
- **Expose the boundary burst problem** in documentation and in the UI — this is educational
- **Distributed**: Redis INCR + EXPIRE. Key = `{key}:{window_epoch}`. On first INCR (result=1), set EXPIRE to `window` duration. Fully atomic with no Lua needed.

---

### Algorithm 6: GCRA (Generic Cell Rate Algorithm)

**Theory:** GCRA is the leaky bucket implemented via a "virtual scheduling" approach. Instead of a queue, it tracks a single timestamp: the Theoretical Arrival Time (TAT) of the next allowed request. Each request computes its TAT, compares with now, and is allowed if TAT <= now + burst. This is what Stripe, Shopify, and many high-performance systems use.

**Formula:**
```
TAT_new = max(TAT_old, now) + emission_interval
emission_interval = window / limit  (e.g., 1 request per 100ms for 10 req/s)
allowed = TAT_new <= now + burst_offset
burst_offset = emission_interval * (burst - 1)
```

**Implementation requirements (`ratelimit/gcra/gcra.go`):**

```go
type GCRA struct {
    emissionInterval time.Duration  // window / limit
    burstOffset      time.Duration  // emission_interval * (burst - 1)
    tat              map[string]time.Time  // Theoretical Arrival Time per key
    mu               sync.RWMutex
    clock            Clock
}
```

- On `Allow(key)`:
  1. `tat_old = max(tat[key], now)`
  2. `tat_new = tat_old + emissionInterval`
  3. If `tat_new - burstOffset > now` → denied. `RetryAfter = tat_new - burstOffset - now`
  4. Store `tat[key] = tat_new`
  5. `Remaining = floor((now + burstOffset - tat_new) / emissionInterval)`
- **Why GCRA is superior**: no floating point arithmetic, single timestamp per key (extremely memory efficient), works perfectly for distributed systems (one Redis SET per key)
- **Distributed** (`gcra/distributed.go`): Redis GET tat, compute in Go, SET tat — use `SET key value XX GET` (Redis 6.2+) for CAS-like behavior. Use Lua script for strict atomicity on older Redis.
- **Expose in Metadata**: `tat`, `tat_new`, `emission_interval_ms`, `burst_offset_ms`, `remaining`

---

### Algorithm 7: Adaptive Rate Limiter

**Theory:** Dynamically adjust the rate limit based on system signals — CPU usage, error rate, latency percentiles. Backs off limit when system is stressed, increases when healthy. Used internally by systems like Netflix Concurrency Limiter.

**Implementation requirements (`ratelimit/adaptive/adaptive.go`):**

```go
type AdaptiveLimiter struct {
    base     Limiter         // Underlying limiter (token bucket by default)
    min      int             // Minimum limit (floor)
    max      int             // Maximum limit (ceiling)
    current  atomic.Int64    // Current effective limit
    signals  SignalSource    // Where to read system health from
    clock    Clock
    done     chan struct{}
}

type SignalSource interface {
    CPUPercent() float64   // 0-100
    ErrorRate() float64    // 0-1 (fraction of recent requests that errored)
    P99Latency() time.Duration
}
```

- Background goroutine re-evaluates limit every `AdjustInterval` (default: 1s):
  - If `CPUPercent > 80` or `ErrorRate > 0.05` or `P99Latency > threshold` → decrease limit by 10%, floor at min
  - If `CPUPercent < 50` and `ErrorRate < 0.01` and `P99Latency < threshold * 0.5` → increase limit by 5%, ceiling at max
  - Apply gradient smoothing: `new_limit = current * 0.9 + target * 0.1` — avoid oscillation
- **Real signal sources**: provide implementations for CPU (using `runtime` package) and error rate (exponential moving average of request outcomes reported via `RecordSuccess()` / `RecordError()` methods)
- Default signal source uses `runtime.ReadMemStats` for GC pressure as a proxy for system load

---

### Algorithm 8: Composite / Chained Limiter

**Implementation requirements (`ratelimit/composite/composite.go`):**

```go
type CompositeLimiter struct {
    limiters []Limiter
    mode     CompositeMode  // AND or OR
}

// AND: all limiters must allow (strictest — most common for multi-tier limiting)
// OR: any limiter allowing is sufficient
type CompositeMode int
const (
    AND CompositeMode = iota
    OR
)
```

Use case: `composite.New(AND, globalLimiter, perUserLimiter, perEndpointLimiter)` — all three must allow.

On deny: return the most restrictive Result (smallest Remaining, longest RetryAfter).

---

## LIBRARY SPECIFICATIONS — CIRCUIT BREAKER

### Core State Machine

```
        ┌─────────────────────────────────────────────────┐
        │                                                 │
        ▼                                                 │
  ┌──────────┐   failure_threshold exceeded   ┌────────┐  │
  │  CLOSED  │ ─────────────────────────────► │  OPEN  │  │
  │(normal)  │                                │(reject)│  │
  └──────────┘                                └────────┘  │
        ▲                                         │        │
        │                                         │ after  │
        │ success_threshold                       │ open_  │
        │ exceeded                                │ timeout│
        │                                         ▼        │
        │                                   ┌──────────┐   │
        └──────────────────────────────────│ HALF-OPEN │───┘
                                            │ (probe)  │  failure
                                            └──────────┘
```

**Implementation requirements (`circuitbreaker/circuitbreaker.go`):**

```go
type CircuitBreaker struct {
    name    string
    config  Config
    state   atomic.Int32    // State enum stored atomically
    metrics *windowMetrics  // Rolling window failure tracking
    mu      sync.Mutex      // Only needed for state transitions
    openedAt time.Time      // When circuit was last opened
    halfOpenRequests atomic.Int32  // Concurrent probes in half-open
    clock   Clock

    // Lifecycle callbacks
    onStateChange   func(name string, from, to State)
    onSuccess       func(name string, duration time.Duration)
    onFailure       func(name string, err error, duration time.Duration)
    onRejected      func(name string)
}

type State int32
const (
    StateClosed   State = iota
    StateHalfOpen
    StateOpen
)
```

**Config (`circuitbreaker/config.go`):**

```go
type Config struct {
    // Failure detection
    FailureThreshold      int           // failures before opening (e.g., 5)
    FailureRateThreshold  float64       // OR: failure rate % before opening (e.g., 0.5 = 50%)
    MinimumRequests       int           // minimum requests before evaluating rate threshold
    WindowType            WindowType    // COUNT_BASED or TIME_BASED
    WindowSize            int           // count-based: last N requests; time-based: last N seconds

    // Recovery
    OpenTimeout           time.Duration // how long to stay open before half-open probe
    HalfOpenMaxRequests   int           // concurrent probes allowed in half-open (default: 1)
    SuccessThreshold      int           // successes in half-open to close circuit

    // Timeout
    RequestTimeout        time.Duration // max duration before counting as failure (0 = disabled)

    // Callbacks
    OnStateChange  func(name string, from, to State)
    OnSuccess      func(name string, duration time.Duration)
    OnFailure      func(name string, err error, duration time.Duration)
    OnRejected     func(name string)

    // Clock
    Clock Clock
}
```

**Two window types for failure tracking (`circuitbreaker/metrics.go`):**

1. **Count-based window**: ring buffer of last N request outcomes (success/failure). Compact, O(1) operations.
2. **Time-based window**: fixed buckets (e.g., 10 x 1s = 10s window). Each second, add new bucket and discard oldest. Track total success/failure across all buckets.

```go
type windowMetrics struct {
    windowType  WindowType
    // Count-based: ring buffer
    ring        []outcome
    head        int
    // Time-based: buckets
    buckets     []bucket
    bucketWidth time.Duration
    mu          sync.Mutex
}
```

**State transition logic (`circuitbreaker/state.go`):**

- `Closed → Open`:
  - Count-based: if last N requests have >= `FailureThreshold` failures OR failure rate >= `FailureRateThreshold` (only if `len >= MinimumRequests`)
  - Time-based: if in rolling window failure rate >= threshold with sufficient volume
  - On transition: record `openedAt = now`, invoke `onStateChange`

- `Open → HalfOpen`:
  - After `OpenTimeout` has elapsed since `openedAt`
  - First request that arrives after timeout triggers transition (lazy)
  - Atomically transition using `atomic.CompareAndSwap`

- `HalfOpen → Closed`:
  - After `SuccessThreshold` consecutive successes
  - Reset all metrics

- `HalfOpen → Open`:
  - On any failure
  - Reset `openedAt = now` (full timeout restarts)

- **Concurrent half-open probes**: `HalfOpenMaxRequests` allows multiple probes simultaneously. Use `atomic.Add` to track and limit probe count. Requests exceeding the limit are **rejected** (not failed — important distinction).

**Execute method:**

```go
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
    // 1. Check state — reject immediately if open
    // 2. If half-open, check probe limit
    // 3. Execute fn with optional timeout from config
    // 4. Record outcome (success/failure) and transition state if thresholds met
    // 5. Call appropriate callback
}

// ExecuteWithFallback runs fn and calls fallback(err) if circuit is open or fn returns error
func (cb *CircuitBreaker) ExecuteWithFallback(
    ctx context.Context,
    fn func(ctx context.Context) error,
    fallback func(ctx context.Context, err error) error,
) error
```

**Circuit Breaker Registry (`circuitbreaker/registry.go`):**

```go
var Global = NewRegistry()

type Registry struct {
    breakers sync.Map  // string → *CircuitBreaker
}

func (r *Registry) GetOrCreate(name string, cfg Config) *CircuitBreaker
func (r *Registry) Get(name string) (*CircuitBreaker, bool)
func (r *Registry) Snapshot() map[string]Snapshot  // all breakers' current state
func (r *Registry) Reset(name string)
func (r *Registry) ResetAll()
```

---

## LIBRARY SPECIFICATIONS — SUPPORTING PATTERNS

### Bulkhead (`bulkhead/bulkhead.go`)

Limit concurrent executions to prevent cascading failures when one dependency is slow.

```go
type Bulkhead struct {
    sem      chan struct{}  // Semaphore (buffered channel)
    maxWait  time.Duration // 0 = no waiting (reject immediately), >0 = queue with timeout
    inflight atomic.Int64  // Current inflight count for observability
}

func (b *Bulkhead) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
    // Acquire semaphore (with timeout if maxWait > 0)
    // Execute fn
    // Release semaphore (defer)
}
```

Expose: `Inflight()`, `Available()`, `QueueDepth()` (if maxWait > 0).

**Thread Pool Bulkhead** (`bulkhead/threadpool.go`): Fixed worker pool with a task queue. Callers submit tasks and optionally wait for result via a result channel. Prevents goroutine explosion under load.

---

### Retry (`retry/retry.go`)

```go
type Policy struct {
    MaxAttempts  int
    Backoff      BackoffStrategy
    RetryIf      func(err error) bool   // nil = retry all non-nil errors
    OnRetry      func(attempt int, err error, nextWait time.Duration)
}

func Do(ctx context.Context, policy Policy, fn func(ctx context.Context) error) error
func DoWithResult[T any](ctx context.Context, policy Policy, fn func(ctx context.Context) (T, error)) (T, error)
```

**Backoff strategies (all implement `BackoffStrategy` interface → `Next(attempt int) time.Duration`):**

1. **Constant** (`backoff/constant.go`): always `delay`
2. **Exponential** (`backoff/exponential.go`): `min(base * 2^attempt, max)`
3. **Full Jitter** (`backoff/jitter.go`): `random(0, min(base * 2^attempt, max))` — AWS recommended for distributed retries, avoids thundering herd
4. **Equal Jitter** (`backoff/jitter.go`): `cap/2 + random(0, cap/2)` — always some backoff, some randomness
5. **Decorrelated Jitter** (`backoff/decorrelated.go`): `min(max, random(base, previous * 3))` — best convergence properties per AWS paper

Document which strategy to use when (full jitter for distributed, exponential for local, decorrelated for best long-term behavior).

---

### Timeout (`timeout/timeout.go`)

```go
func WithTimeout(ctx context.Context, d time.Duration, fn func(ctx context.Context) error) error {
    ctx, cancel := context.WithTimeout(ctx, d)
    defer cancel()
    
    done := make(chan error, 1)
    go func() { done <- fn(ctx) }()
    
    select {
    case err := <-done:
        return err
    case <-ctx.Done():
        return &TimeoutError{Duration: d, Err: ctx.Err()}
    }
}
```

`TimeoutError` is a typed error — callers can `errors.As(err, &TimeoutError{})` to detect timeouts specifically.

---

### Fallback (`fallback/fallback.go`)

```go
// Execute primary. If it fails, execute fallback.
func WithFallback(ctx context.Context,
    primary func(ctx context.Context) error,
    fallback func(ctx context.Context, cause error) error,
    opts ...Option,
) error

// Hedge: fire primary, then after hedgeDelay fire backup. Return whichever completes first.
// Cancel the loser. Used to reduce P99 tail latency.
func Hedge(ctx context.Context,
    hedgeDelay time.Duration,
    fn func(ctx context.Context) error,
    opts ...Option,
) error
```

---

### Resilience Pipeline (`pipeline/pipeline.go`)

Builder pattern to compose all patterns:

```go
// Creates: RateLimit → Bulkhead → Timeout → CircuitBreaker → Retry → Execute
pipeline := resilience.NewPipeline().
    WithRateLimiter(tokenBucketLimiter, "user-id").
    WithBulkhead(50, 10*time.Millisecond).
    WithTimeout(5*time.Second).
    WithCircuitBreaker(cb).
    WithRetry(policy).
    Build()

err := pipeline.Execute(ctx, func(ctx context.Context) error {
    return callDownstreamService(ctx)
})
```

Order matters — rate limiting before bulkhead prevents unnecessary lock contention. Retry is innermost (retries the individual call, not the rate limit check).

---

## DEMO SERVER SPECIFICATIONS

The demo server is a Go HTTP server that exercises the library and provides data for the frontend.

### Endpoint Groups

**Rate Limiter Endpoints:**

```
POST /api/limiter/:algorithm/allow          → call Allow() once, return Result
POST /api/limiter/:algorithm/allow-n        → call AllowN(n), return Result
POST /api/limiter/:algorithm/wait           → call Wait() (with ctx timeout), return latency
POST /api/limiter/:algorithm/reset          → reset state for key
GET  /api/limiter/:algorithm/state          → call Peek(), return State
POST /api/limiter/:algorithm/configure      → reconfigure limit/window at runtime

# algorithms: token_bucket, leaky_bucket, sliding_window_log,
#             sliding_window_counter, fixed_window, gcra, adaptive, composite
```

**Circuit Breaker Endpoints:**

```
GET  /api/cb/:name/state                   → current state (closed/open/half-open)
POST /api/cb/:name/execute                 → execute a simulated request through the CB
POST /api/cb/:name/force-open              → manually trip the circuit (for demo)
POST /api/cb/:name/force-close             → manually reset the circuit
POST /api/cb/:name/configure               → change thresholds at runtime
GET  /api/cb/:name/metrics                 → full metrics snapshot
GET  /api/cb/all                           → snapshot of all registered breakers
```

**Load Simulation Endpoints:**

```
POST /api/simulate/burst                   → send N requests in parallel, return aggregate results
POST /api/simulate/gradual                 → ramp from 0 to N req/s over duration
POST /api/simulate/spike                   → constant load then sudden 10x spike
POST /api/simulate/failure-injection       → simulate downstream errors at configurable rate
POST /api/simulate/stop                    → stop any running simulation
GET  /api/simulate/status                  → current simulation status
```

**WebSocket — Real-time State Streaming:**

```
WS /ws/limiter/:algorithm                  → stream State every 100ms
WS /ws/cb/:name                            → stream CB state + metrics every 100ms
WS /ws/events                              → stream all allow/deny/state-change events
WS /ws/simulation                          → stream simulation progress in real time
```

**WebSocket event schema:**
```typescript
interface Event {
  id: string
  timestamp: string
  type: 'allow' | 'deny' | 'wait_start' | 'wait_end' | 'cb_state_change' | 'cb_execute' | 'cb_reject'
  algorithm: string
  key: string
  result?: Result
  state?: State
  cbTransition?: { from: string, to: string, reason: string }
  latency_ms?: number
}
```

**Prometheus Metrics:**

```
/metrics  →  Prometheus scrape endpoint

# Rate limiter metrics
resilience_ratelimit_requests_total{algorithm, key, result}
resilience_ratelimit_wait_duration_seconds{algorithm, key}  (histogram)
resilience_ratelimit_tokens_remaining{algorithm, key}       (gauge)

# Circuit breaker metrics
resilience_cb_state{name}                                   (gauge: 0=closed, 1=half-open, 2=open)
resilience_cb_requests_total{name, result}                  (result: success, failure, rejected)
resilience_cb_failure_rate{name}                            (gauge)
resilience_cb_state_transitions_total{name, from, to}

# Retry metrics
resilience_retry_attempts_total{result}
resilience_retry_duration_seconds{result}                   (histogram)

# Bulkhead metrics
resilience_bulkhead_inflight{name}                          (gauge)
resilience_bulkhead_rejected_total{name}
```

---

## FRONTEND SPECIFICATIONS (Next.js)

### Stack

- **Next.js 14+** with App Router
- **TypeScript** strict mode, zero `any`
- **Tailwind CSS** + **shadcn/ui** component system
- **Recharts** for all data visualizations
- **SWR** or **React Query (TanStack Query)** for data fetching + polling
- **Zustand** for global simulation state
- Dark-first design: dense, technical, developer tool aesthetic

### Design System

```css
/* Dark developer tool palette */
--bg-base:      #0a0a0b;   /* deepest background */
--bg-surface:   #111113;   /* card/panel backgrounds */
--bg-elevated:  #18181b;   /* modals, dropdowns */
--border:       #27272a;   /* subtle borders */
--text-primary: #fafafa;
--text-muted:   #71717a;
--accent:       #6366f1;   /* indigo — primary brand */

/* Algorithm color coding (consistent across all views) */
--algo-token:   #3b82f6;   /* blue — token bucket */
--algo-leaky:   #10b981;   /* emerald — leaky bucket */
--algo-swlog:   #f59e0b;   /* amber — sliding window log */
--algo-swctr:   #f97316;   /* orange — sliding window counter */
--algo-fixed:   #ec4899;   /* pink — fixed window */
--algo-gcra:    #8b5cf6;   /* violet — GCRA */
--algo-adaptive:#14b8a6;   /* teal — adaptive */

/* Circuit breaker state colors */
--cb-closed:    #22c55e;   /* green */
--cb-half-open: #f59e0b;   /* amber */
--cb-open:      #ef4444;   /* red */
```

Typography: `JetBrains Mono` for all numeric values, state values, code. `Inter` for UI chrome.

### Application Routes

```
/                           → Overview: all algorithms + CBs at a glance
/algorithms/token_bucket    → Token bucket deep dive
/algorithms/leaky_bucket    → Leaky bucket deep dive
/algorithms/sliding_window  → Sliding window (log + counter tabs)
/algorithms/fixed_window    → Fixed window counter
/algorithms/gcra            → GCRA deep dive
/algorithms/adaptive        → Adaptive limiter
/algorithms/compare         → Side-by-side algorithm comparison
/circuit-breaker            → Circuit breaker state machine
/circuit-breaker/[name]     → Individual CB detail
/pipeline                   → Resilience pipeline builder
/simulate                   → Load simulator
/docs                       → Algorithm documentation with math
```

---

### Page Specifications

#### Overview Page (`/`)

**Top bar:** Global simulation status, active rate limiters count, open circuit breakers count (with red badge if any are open).

**Algorithm cards grid (2x4):** One card per algorithm. Each shows:
- Algorithm name + color indicator
- Current config (limit, window)
- Tokens remaining / queue depth (live, polling every 500ms)
- Last 60 seconds sparkline (allow vs deny)
- "Open" button → navigates to deep dive

**Circuit Breaker summary row:** All registered CBs as state badges (green/amber/red). Click → navigates to CB detail.

---

#### Algorithm Deep Dive Pages (`/algorithms/[algo]`)

Each algorithm page has a consistent 3-panel layout:

**Left panel (30%) — Controls + Config:**
- Reconfigure form: limit, window size, burst (for token bucket)
- Manual request buttons: `Send 1 Request`, `Send 10 Requests`, `Wait for Token`
- Result of last request: big ALLOWED / DENIED badge with full Result breakdown
- Remaining tokens / current queue depth (large numeric display, live)
- Reset state button

**Center panel (40%) — Algorithm Visualizer:**

Each algorithm has a custom visualization:

- **Token Bucket**: animated bucket graphic with water level (tokens). Tokens drip in at the refill rate (animation). Requests consume tokens (animated drain). Shows capacity, current level, refill rate numerically. When a request is denied, the bucket shakes.

- **Leaky Bucket**: animated queue visualization. Requests enter from the top as colored dots. They queue up. The leak hole at the bottom drips at constant rate. When queue is full, new requests bounce off (denied). Queue depth number displayed prominently.

- **Sliding Window Log**: scrolling timeline showing the last N seconds. Individual request dots on the timeline. Window boundary shown as a vertical line moving right. Dots older than the window fade out. Current count displayed. You can see exactly which requests are "in window."

- **Sliding Window Counter**: two side-by-side bucket visualization. "Previous Window" (faded, weighted) and "Current Window" (bright). Arrow showing the weight interpolation. Formula displayed live: `previous * (1 - elapsed/window) + current = effective_count`.

- **Fixed Window Counter**: big counter display, progress bar from 0 to limit. Window countdown timer (time until reset). Red flash when denied. When window resets, counter animates back to 0.

- **GCRA**: timeline showing the Theoretical Arrival Time (TAT) pointer moving forward. Each allowed request advances TAT by `emission_interval`. Shows the "credit corridor" — the window where requests are allowed. Burst capacity visualized as extra corridor width.

- **Adaptive Limiter**: triple gauge display: CPU%, Error Rate%, P99 Latency. Current effective limit shown as a gauge with min/max bounds. Animation when limit adjusts up or down. Graph of limit over time.

**Right panel (30%) — Live Event Stream:**
- Last 50 events as a scrolling feed (auto-scroll, pause on hover)
- Each event: timestamp, ALLOWED/DENIED badge, key, remaining, retryAfter
- Color-coded: green for allowed, red for denied, amber for wait
- Event rate counter (events/sec)
- Latency histogram (p50/p95/p99 of Allow() call duration)

**Below the 3 panels — Algorithm Explanation:**
- Mathematical formula with rendered LaTeX (use `KaTeX`)
- Complexity table: time complexity, space complexity, distribution support, burst support
- Pros/cons in two columns
- Code snippet showing how to use this limiter in a Go application

---

#### Algorithm Comparison Page (`/algorithms/compare`)

Select 2-4 algorithms. Configure each with the same limit/window. Run the same request sequence against all simultaneously. See:

- Side-by-side result comparison (same request, different outcomes per algorithm)
- Latency comparison table
- Under a "Boundary Burst" test: show how fixed window allows 2x requests at boundary while sliding window does not
- Under a "Burst" test: show how token bucket allows bursting while leaky bucket smooths it
- Export comparison results as CSV

---

#### Circuit Breaker Page (`/circuit-breaker`)

**State Machine Diagram (center):**
Interactive SVG state machine diagram:
- Three nodes: CLOSED (green circle), HALF-OPEN (amber circle), OPEN (red circle)
- Animated arrows showing transitions
- **Currently active state has a pulsing glow animation**
- Hover any arrow → shows transition condition tooltip
- The diagram updates live — when you trigger a state change via the controls, the diagram animates the transition

**Controls (left):**
- "Simulate Success" button → calls Execute() with a function that succeeds
- "Simulate Failure" button → calls Execute() with a function that returns an error
- "Simulate Timeout" button → calls Execute() with a function that exceeds timeout
- "Force Open" / "Force Close" buttons
- Slider: "Error injection rate" (0-100%) for bulk simulation
- Config editor: failure threshold, rate threshold, window size, open timeout, half-open max requests

**Metrics panel (right):**
- Failure rate (last window): large percentage display
- Requests in window: count / window size
- Time until half-open: countdown when circuit is open
- Consecutive successes: count / threshold when half-open
- Rejection count (session)
- All-time state transition log (timestamp, from, to, reason)

**Live Metrics Charts (bottom):**
- Requests/sec (success vs failure vs rejected) — stacked area chart, last 2 minutes
- Failure rate over time — line chart with threshold line shown
- Circuit state over time — step chart (0=closed, 0.5=half-open, 1=open)

---

#### Circuit Breaker Detail Page (`/circuit-breaker/[name]`)

Same layout as above but scoped to one named circuit breaker with its specific config visible.

---

#### Load Simulator (`/simulate`)

The most powerful page — run scripted load scenarios and watch everything respond live.

**Scenario Selector:**
Pre-built scenarios (tabs):

1. **Burst Test**: 100 req/s for 10s, then sudden 500 req/s spike for 5s, back to 100
2. **Gradual Ramp**: ramp from 0 to 1000 req/s over 60s
3. **Failure Injection**: constant load + inject 50% errors every 10s → watch circuit breaker trip and recover
4. **Thundering Herd**: all requests fire simultaneously (tests burst behavior)
5. **Custom**: configure all parameters manually

**Simulation Config Panel:**
- Target algorithm + circuit breaker
- Request rate (req/s), duration, concurrency
- Error injection rate (%)
- Scenario type + parameters

**Live Simulation Dashboard (runs when simulation is active):**

Full-width charts updating every 100ms via WebSocket:

- **Request Timeline**: real-time scatter plot. X=time, Y=latency. Green dots=allowed, Red=denied, Gray=rejected(CB). Zoom and pan.
- **Rate Chart**: actual req/s received vs. allowed per second — area chart
- **Circuit Breaker State**: step chart (closed=0 / half-open=0.5 / open=1) overlaid on the timeline
- **Algorithm Internals**: live visualization of the selected algorithm (token level, TAT, etc.) during the load test — shows how it responds to the load
- **Aggregate Stats**: total sent, total allowed (%), total denied (%), total rejected by CB, p50/p95/p99 latency

**Results Summary (after simulation ends):**
- Tabular breakdown: per-second stats
- Export as JSON / CSV
- "Replay" button — re-run exact same scenario

---

#### Resilience Pipeline Builder (`/pipeline`)

Drag-and-drop pipeline builder:

**Stages (draggable cards in order):**
1. Rate Limiter (select algorithm, configure)
2. Bulkhead (concurrency limit)
3. Timeout
4. Circuit Breaker (select or create named CB)
5. Retry (select backoff strategy, configure)

Each stage can be toggled on/off with a toggle. Config panel slides out when you click a stage card.

**Pipeline Execution:**
- "Send Request" button → sends through the full pipeline
- "Send 100 Requests" → shows how many make it through each stage (funnel visualization)
- Funnel chart: N entered → N passed rate limit → N passed bulkhead → N passed CB → N succeeded → N retried → N total successes

---

#### Documentation Pages (`/docs`)

Static MDX pages (using `next-mdx-remote` or `contentlayer`) with:
- Algorithm mathematical foundations (LaTeX rendered with KaTeX)
- Visual complexity comparison tables
- "When to use X vs Y" decision trees
- Code examples (syntax highlighted with `shiki`)
- Links to source files in the repository

---

### Frontend Architecture Details

**API Client (`lib/api/client.ts`):**
```typescript
// Typed API client with full Result/State/Event type definitions
// Base URL from NEXT_PUBLIC_API_URL env var
// All endpoints return typed responses
// Error handling: all API errors are typed ApiError with status + message
// Retry on 5xx (max 2 retries, exponential backoff in the client)
```

**WebSocket Manager (`lib/ws/manager.ts`):**
```typescript
class WSManager {
  private connections: Map<string, WebSocket>
  
  subscribe(endpoint: string, onEvent: (e: Event) => void): () => void
  // Returns unsubscribe function
  // Auto-reconnects with exponential backoff (1s, 2s, 4s, max 30s)
  // Buffers events during reconnect window
  // Exposes connection state as a reactive store
}
```

**Algorithm Animations:**
- Use `Framer Motion` for all algorithm visualizations
- All animations respect `prefers-reduced-motion` (instant state change when enabled)
- Animation speed configurable in settings (0.5x, 1x, 2x, 5x)

**State Management (Zustand):**
```typescript
// Global stores:
useSimulationStore  // active simulation state, progress, results
useAlgorithmStore   // per-algorithm config and live state cache
useCBStore          // circuit breaker states
useSettingsStore    // animation speed, polling interval, theme prefs
```

**Error & Loading States:**
Every data-dependent component must handle:
- Loading: skeleton matching content shape
- Error: inline error with retry button
- Empty: meaningful empty state (no requests sent yet → "Configure and send your first request")
- Stale data: show timestamp of last successful fetch, indicator if WS is disconnected

**Accessibility:**
- All state changes announced via `aria-live` regions
- All charts have text alternatives (data tables behind "View as table" button)
- Full keyboard navigation for pipeline builder (arrow keys for drag, Enter/Space to select)
- WCAG 2.1 AA

---

## TESTING REQUIREMENTS

### Go Library Tests

**Unit tests (>85% coverage required):**
Every algorithm must have tests for:
- Basic allow/deny at exact limit boundary
- AllowN (n=1, n=5, n=limit, n=limit+1)
- Time progression (using mock clock): allow, advance time, verify refill
- Wait() with context cancellation
- Wait() with timeout
- Concurrent access: 100 goroutines calling Allow simultaneously — race detector must pass
- Reset() clears state
- Composite: AND mode (both must allow), OR mode (either allows)

**Race condition tests (all must pass `go test -race`):**
Every algorithm, every public method called from multiple goroutines simultaneously.

**Fuzz tests:**
```go
// fuzz_test.go
func FuzzTokenBucketAllowN(f *testing.F) {
    f.Add(100, 10, int64(time.Second), 5)  // capacity, refillRate, elapsed, n
    f.Fuzz(func(t *testing.T, capacity, rate int, elapsed int64, n int) {
        if capacity <= 0 || rate <= 0 || n <= 0 { return }
        bucket := tokenbucket.New(capacity, rate)
        clock.Advance(time.Duration(elapsed))
        result := bucket.AllowN(ctx, "key", n)
        // Must never panic
        // Remaining must be >= 0 and <= capacity
        if result.Remaining < 0 || result.Remaining > capacity {
            t.Errorf("invariant violated: remaining=%d capacity=%d", result.Remaining, capacity)
        }
    })
}
```

**Benchmark tests (all must meet targets):**
```go
// Target: <200ns/op for in-memory Allow(), <1μs/op for AllowN
BenchmarkTokenBucketAllow           // target: <200ns/op
BenchmarkTokenBucketAllowParallel   // GOMAXPROCS goroutines, target: <500ns/op
BenchmarkLeakyBucketAllow           // target: <300ns/op
BenchmarkSlidingWindowLogAllow      // target: <1μs/op (expected slower due to slice ops)
BenchmarkSlidingWindowCounterAllow  // target: <200ns/op
BenchmarkFixedWindowAllow           // target: <150ns/op (simplest)
BenchmarkGCRAAllow                  // target: <200ns/op
BenchmarkCircuitBreakerExecute      // target: <500ns/op (closed state, no contention)
BenchmarkCircuitBreakerExecuteParallel // 1000 goroutines, target: <2μs/op
```

**Integration tests** (`test/integration/`):
- Start a real HTTP server with rate limiting middleware
- Send real HTTP requests in parallel
- Verify rate limiting behavior end-to-end
- Circuit breaker integration: inject failures into downstream handler, verify CB state transitions

**Redis integration tests** (using `testcontainers-go`):
- Start a real Redis instance in a Docker container
- Test distributed rate limiting across 5 concurrent "clients" (goroutines sharing nothing)
- Verify global rate limit is respected (not 5x limit)
- Verify behavior during Redis unavailability (fallback strategy)

---

## PRODUCTION QUALITY REQUIREMENTS

### API Stability & Documentation

The library must be production-publishable:
- **godoc** comments on every exported type, function, method, and constant
- **Example functions** (`Example*` in `_test.go`) for every algorithm showing basic usage — these appear in godoc
- **README.md** with: quick start code snippet, algorithm comparison table, when-to-use guide, performance benchmarks, API reference
- **CHANGELOG.md** with semantic versioning
- Version: `v1.0.0` — this is a stable release, no breaking changes after tagging
- Module path: `github.com/sanskarpan/resilience`

### Zero External Runtime Dependencies (core library)

`go.mod` for the core library packages must have zero `require` entries. Pure stdlib only:
- `sync`, `sync/atomic` for concurrency
- `time` for clock operations
- `context` for cancellation
- `math` for calculations
- `log/slog` for optional structured logging

The Redis adapter (`store/redis.go`) is a separate import path `github.com/sanskarpan/resilience/store/redis` with `go-redis/v9` as its only dependency — keeping the core library zero-dependency.

### Thread Safety Guarantees

Every public method on every type must be documented as safe for concurrent use. The documentation must state: `All methods on [Type] are safe for concurrent use.`

Internally:
- Use `atomic` operations for hot-path single-value reads/writes (token count, state enum)
- Use `sync.Mutex` only when multiple fields must be updated atomically
- Use `sync.RWMutex` when reads dominate (allow read concurrency)
- Document which lock protects which fields in comments

### Error Types

All errors returned by the library are typed and inspectable:

```go
var (
    ErrLimitExceeded  = errors.New("rate limit exceeded")    // base error
    ErrCircuitOpen    = errors.New("circuit breaker is open")
    ErrBulkheadFull   = errors.New("bulkhead capacity reached")
    ErrTimeout        = errors.New("operation timed out")
)

type RateLimitError struct {
    Algorithm  string
    Key        string
    Limit      int
    RetryAfter time.Duration
    Err        error
}
func (e *RateLimitError) Error() string { ... }
func (e *RateLimitError) Unwrap() error { return e.Err }
func (e *RateLimitError) Is(target error) bool { return target == ErrLimitExceeded }
```

Users can `errors.Is(err, ratelimit.ErrLimitExceeded)` or `errors.As(err, &RateLimitError{})`.

---

---

## PRODUCTION HARDENING

### Goroutine Leak Detection

Every type that spawns goroutines (LeakyBucket, AdaptiveLimiter, memory store cleanup, WebSocket hub) must be verified to release them cleanly on `Close()`.

**`internal/testutil/goroutine.go`:**
```go
// LeakChecker captures goroutine count before a test and verifies no net
// increase after. A difference of ±2 is tolerated for Go scheduler goroutines.
type LeakChecker struct {
    before int
    t      testing.TB
}

func NewLeakChecker(t testing.TB) *LeakChecker {
    runtime.GC()
    time.Sleep(10 * time.Millisecond)
    return &LeakChecker{before: runtime.NumGoroutine(), t: t}
}

func (c *LeakChecker) Check() {
    runtime.GC()
    time.Sleep(50 * time.Millisecond)
    after := runtime.NumGoroutine()
    if diff := after - c.before; diff > 2 {
        buf := make([]byte, 1<<20)
        n := runtime.Stack(buf, true)
        c.t.Errorf("goroutine leak: +%d goroutines. Stack:\n%s", diff, buf[:n])
    }
}
```

**Required usage:** Add `defer testutil.NewLeakChecker(t).Check()` at the start of every test for: LeakyBucket, AdaptiveLimiter, memory.Store, WebSocket hub, simulation engine.

### Chaos Testing for Algorithm Invariants

**`internal/testutil/chaos.go`:** Runs each algorithm under 500 concurrent goroutines for 1M operations and verifies all invariants hold. Required for all algorithms before Phase 1 is complete.

Invariants to verify:
- Token Bucket: `0 <= remaining <= capacity` on every result
- GCRA: `0 <= remaining <= burst` on every result
- Circuit Breaker: state is always one of `{0=Closed, 1=HalfOpen, 2=Open}`, never any other value
- Sliding Window: `0 <= count <= limit` on every result

### HTTP Rate Limit Response Headers — Full Specification

Every rate limit middleware response (both 200 and 429) must set:

```
X-RateLimit-Limit: 100              # configured limit
X-RateLimit-Remaining: 95           # after this request (0 on 429)
X-RateLimit-Reset: 1706745600       # unix timestamp of next full reset
X-RateLimit-Policy: 100;w=60        # IETF draft: "limit;w=window_seconds"
Retry-After: 1.500                  # ONLY on 429, decimal seconds to next allowed
```

Header values must match the JSON body values exactly. Test this explicitly.

### Input Validation — Security

All key parameters: non-empty, max 512 bytes, no null bytes (`\x00`), no CR/LF (`\r\n`). CR/LF rejection prevents HTTP response header injection. Return 400 with JSON error body on any violation.

All numeric parameters: validate positive, within reasonable bounds (limit: 1–1M, window: 1ms–24h). Return 400 before any business logic executes.

---

## INFRASTRUCTURE

### Dockerfile (multi-stage)

```dockerfile
# Stage 1: Build Next.js frontend
FROM node:22-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci --frozen-lockfile
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Go demo server
FROM golang:1.23-alpine AS go-builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend-builder /app/frontend/.next ./server/static/.next
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always)" \
    -o bin/demo-server ./server/

# Stage 3: Minimal runtime
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=go-builder /app/bin/demo-server /demo-server
EXPOSE 8080/tcp
USER nonroot:nonroot
ENTRYPOINT ["/demo-server"]
```

### docker-compose.yml

```yaml
version: "3.9"
services:
  demo-server:
    build: .
    ports:
      - "8080:8080"
    environment:
      - LOG_LEVEL=info
      - LOG_FORMAT=json
      - REDIS_URL=redis://redis:6379
      - PROMETHEUS_ENABLED=true
    depends_on:
      redis:
        condition: service_healthy
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 256M

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

  prometheus:
    image: prom/prometheus:v2.50.0
    ports:
      - "9090:9090"
    volumes:
      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro

  grafana:
    image: grafana/grafana:10.3.0
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./deploy/grafana-dashboard.json:/etc/grafana/provisioning/dashboards/resilience.json:ro

volumes:
  redis-data:
  grafana-data:
```

### Makefile

```makefile
.PHONY: dev build test test-race fuzz bench lint docker clean

dev:
	@echo "Starting backend + frontend dev servers..."
	@(cd server && air) & (cd frontend && npm run dev) & wait

build:
	cd frontend && npm run build
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/demo-server ./server/

test:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | grep total
	cd frontend && npm run check

test-race:
	go test -race ./...

fuzz:
	go test -fuzz=FuzzTokenBucketAllowN -fuzztime=30s ./ratelimit/tokenbucket/
	go test -fuzz=FuzzGCRAAllow         -fuzztime=30s ./ratelimit/gcra/
	go test -fuzz=FuzzCircuitBreaker    -fuzztime=30s ./circuitbreaker/

bench:
	go test -bench=. -benchmem -count=3 ./... | tee bench.txt
	@echo "Benchmark complete. Results in bench.txt"

bench-compare:  ## Usage: make bench-compare OLD=main NEW=feature-branch
	git stash && git checkout $(OLD) && go test -bench=. -benchmem -count=5 ./... > /tmp/old.txt
	git checkout $(NEW) && git stash pop && go test -bench=. -benchmem -count=5 ./... > /tmp/new.txt
	benchcmp /tmp/old.txt /tmp/new.txt

lint:
	golangci-lint run --timeout 5m
	cd frontend && npm run lint

docker:
	docker build -t resilience-demo:latest .

docker-run:
	docker-compose up

clean:
	rm -rf bin/ frontend/.next frontend/out coverage.out bench.txt
```

### GitHub Actions CI

**`.github/workflows/ci.yml`:**
```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  test-go:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7-alpine
        ports: ["6379:6379"]
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 5s
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.23"
          cache: true
      - run: go mod download
      - run: go vet ./...
      - run: go test -race -coverprofile=coverage.out ./...
      - name: Coverage gate (>85%)
        run: |
          PCT=$(go tool cover -func=coverage.out | grep total | awk '{print $3}' | tr -d '%')
          awk "BEGIN { exit ($PCT < 85) }" || (echo "Coverage $PCT% < 85%" && exit 1)
      - run: go test -fuzz=FuzzTokenBucketAllowN -fuzztime=30s ./ratelimit/tokenbucket/
      - run: go test -bench=. -benchmem ./... | tee bench.txt
      - uses: golangci/golangci-lint-action@v6
        with: { version: latest }

  test-frontend:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: "22", cache: "npm", cache-dependency-path: "frontend/package-lock.json" }
      - run: cd frontend && npm ci --frozen-lockfile
      - run: cd frontend && npm run check   # svelte-check / tsc
      - run: cd frontend && npm run lint
      - run: cd frontend && npm run build

  verify-zero-deps:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.23" }
      - name: Verify core library has zero runtime deps
        run: |
          # Core packages must have no external deps
          for pkg in ratelimit circuitbreaker bulkhead retry timeout fallback pipeline; do
            DEPS=$(go list -f '{{.Imports}}' ./$pkg/... | grep -v "^(sync|time|context|math|errors|fmt|log|io|os|runtime|strings|strconv|sort|unicode|bytes|encoding|reflect|atomic)" | grep -v "^github.com/sanskarpan/resilience" || true)
            if [ -n "$DEPS" ]; then
              echo "Core package $pkg has external deps: $DEPS"
              exit 1
            fi
          done
          echo "✓ Core library has zero external runtime dependencies"
```

**`.github/workflows/release.yml`:**
```yaml
name: Release
on:
  push:
    tags: ["v*"]
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: actions/setup-go@v5
        with: { go-version: "1.23" }
      - name: Run full test suite
        run: go test -race ./...
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          generate_release_notes: true
      - name: Publish to pkg.go.dev
        run: GOPROXY=proxy.golang.org go list -m github.com/sanskarpan/resilience@${{ github.ref_name }}
```

---

## IMPLEMENTATION RULES FOR CLAUDE CODE

### Non-Negotiable Architecture Rules

1. **No external rate limiting or circuit breaker libraries.** `golang.org/x/time/rate`, `sony/gobreaker`, `afex/hystrix-go`, `uber-go/ratelimit`, `juju/ratelimit` — none of these may appear in any core package. Every algorithm is implemented from first principles. Violation = build failure via `verify-zero-deps` CI job.

2. **Zero external dependencies in core library.** `ratelimit/`, `circuitbreaker/`, `bulkhead/`, `retry/`, `timeout/`, `fallback/`, `pipeline/` must compile with pure stdlib. The only allowed external dependency is in `ratelimit/store/redis.go` (`go-redis/v9`) and in `server/` (demo-only). Enforced by `make verify-deps`.

3. **Clock interface everywhere — non-negotiable.** Every `time.Now()`, `time.Sleep()`, `time.NewTimer()`, `time.NewTicker()` call in core library code must go through the `internal/clock.Clock` interface. No exceptions. Without this, every test that involves timing requires `time.Sleep()` which makes the test suite slow (minutes instead of seconds) and flaky (CI machines are slower than dev machines). The `ManualClock` must be able to advance time instantaneously and fire all timers in order.

4. **Atomic operations on the hot path.** `Allow()` is called on every HTTP request in production. The fast path (token available, bucket not empty) must not acquire a mutex. Use `sync/atomic` or `atomicx.AtomicFloat64` for token count reads. Only acquire `sync.Mutex` when a write (refill calculation) is required. Profile with `pprof` if benchmark targets are not met.

5. **Two-phase allow in Composite AND mode.** Composite limiter in AND mode must check all limiters before consuming from any. If limiter A allows and limiter B denies, limiter A's token must NOT be consumed. Implement as: check all (read-only Peek equivalent) → if all allow, consume from all atomically. This is the most common source of bugs in composite limiters.

6. **Error classification for circuit breaker.** Context cancellation (`context.Canceled`, `context.DeadlineExceeded`) must NOT count as a circuit breaker failure. The caller cancelled or timed out — the downstream service may be perfectly healthy. The circuit breaker's own `RequestTimeout` expiry DOES count as a failure. Use `config.IsFailure func(err error) bool` for custom classification with sensible defaults.

7. **State machine atomicity in circuit breaker.** State transitions (`Closed→Open`, `Open→HalfOpen`, `HalfOpen→Closed`) must use `atomic.CompareAndSwap` to prevent race conditions where multiple goroutines simultaneously detect threshold breach and all try to open the circuit. Only the first CAS succeeds — others see the already-transitioned state.

8. **Goroutine inventory.** Every goroutine in the library must be documented with: what it does, what signal stops it, what `WaitGroup` tracks it. Goroutines that are not stopped on `Close()` are goroutine leaks. All tests for types that spawn goroutines must use the `testutil.LeakChecker` to verify zero goroutine leaks after `Close()`.

### Code Quality Rules

9. **Cyclomatic complexity cap: 10.** Enforced by golangci-lint `cyclop` rule. If a function exceeds 10, decompose it. The circuit breaker `Execute()` and the sliding window `Allow()` are the most likely offenders — plan their decomposition before writing.

10. **Function length cap: 80 lines.** Enforced by golangci-lint `funlen` rule. No exceptions. Split into well-named helpers.

11. **All invariants documented and tested under concurrent chaos.** For each algorithm, document the invariants in godoc:
    - Token bucket: `0 <= tokens <= capacity` at all times
    - Sliding window log: `0 <= len(log[key]) <= limit` at all times
    - Circuit breaker: state is always one of `{Closed, HalfOpen, Open}`, never undefined
    Write a chaos test (1000 concurrent goroutines, 100k operations, all operation types) that asserts these invariants after every operation using `testutil.ChaosTest`.

12. **Benchmark targets are hard requirements, not goals.** If a benchmark does not meet its target, do not mark the ticket done. Profile with `go tool pprof`, identify the bottleneck, fix it. Common fixes: lock granularity (per-key locks instead of global), atomic fast path, interface dispatch elimination via type assertion.

13. **Typed errors are the contract.** Users of the library must be able to write `errors.Is(err, ratelimit.ErrLimitExceeded)` and `errors.As(err, &ratelimit.RateLimitError{})`. This must work regardless of how many layers of wrapping occur. Test this explicitly.

### Frontend Rules

14. **The visualizations are the product.** This is an educational tool. Someone unfamiliar with the token bucket algorithm must understand it by watching the animation for 30 seconds — without reading any text. The animation must show: tokens arriving at refillRate, tokens being consumed by requests, the bucket emptying, the deny behavior, the recovery. If the animation does not achieve this, it is not done.

15. **TypeScript strict mode, Zod runtime validation.** `tsconfig.json` has `"strict": true`. Zero `any` types. Zero `@ts-ignore`. All API responses validated at runtime with Zod: `const result = ResultSchema.parse(response)`. If the backend returns unexpected data, the error is caught and shown to the user — not thrown to crash the component.

16. **Three-state components everywhere.** Every component that fetches data has exactly three states: loading (skeleton), error (inline error card with retry), data (rendered content). Empty data is a fourth state — show a meaningful empty state, not a blank screen. Never render `null` silently.

17. **WebSocket resilience.** The UI must remain functional when the WebSocket disconnects. Show a non-blocking "Live data paused — reconnecting..." banner. All previously loaded data stays visible. Reconnect with exponential backoff (500ms, 1s, 2s, 4s, max 30s). When reconnected, resume streaming — no full page reload.

18. **HTTP rate limit response headers on every response.** Every rate limiter endpoint must set the full set: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After` (on 429 only). These must match the values in the JSON response body exactly. The frontend reads these headers and displays them in the UI.

### Production Operations Rules

19. **Health check split.** `/health/live` (liveness) and `/health/ready` (readiness) are different endpoints with different semantics. Liveness: is the process running? Always 200 if the process is alive. Readiness: can it serve traffic? Returns 503 until all components are initialized (limiters configured, Redis connected if enabled). Kubernetes uses these differently — do not combine them.

20. **Graceful shutdown order.** On SIGTERM: (1) mark health/ready = false, (2) stop accepting new requests, (3) wait up to 15s for in-flight requests to complete, (4) close all limiters (stops background goroutines), (5) flush logs, (6) exit 0. Test this with an integration test that sends SIGTERM during active load.

21. **Structured logging mandatory fields.** Every log line emitted during request processing must include: `request_id`, `method`, `path`, `status`, `duration_ms`, `algorithm`, `key`, `allowed`, `client_ip`. Use `log/slog` with JSON handler. Use `logger.FromCtx(ctx)` to get the request-scoped logger. Never use `fmt.Println` or `log.Printf` in server code.

22. **No placeholder implementations.** Every algorithm, every endpoint, every visualization is complete. No `// TODO`. No `panic("not implemented")`. No stub functions. If a ticket says "implement X", X is fully implemented, tested, and documented before the ticket is closed.

---

## STARTING SEQUENCE FOR CLAUDE CODE

> **Important:** This project has a companion `tickets.md` file with 36 granular tickets across 8 phases. Use that file for step-by-step execution. The sequence below maps to those phases.

**Phase 0 — Foundation (tickets T-001 to T-005):**
1. `go.mod` + `.golangci.yml` + `Makefile` (full, not skeleton)
2. `internal/clock/` — Clock interface + RealClock + ManualClock with Advance()
3. `internal/atomicx/float64.go` — atomic float64
4. `ratelimit/limiter.go` — Limiter interface + Result + State
5. `ratelimit/errors.go` — all typed errors
6. `ratelimit/store/store.go` + `memory.go` — Store interface + in-memory impl
7. `internal/testutil/` — LeakChecker + ChaosTest helpers

**Phase 1 — Rate Limiting Algorithms (tickets T-101 to T-108):**
Build in this order (each with full tests + benchmarks before moving on):
8. Token Bucket (most foundational, sets patterns for rest)
9. Fixed Window Counter (simplest after token bucket)
10. Leaky Bucket
11. Sliding Window Log
12. Sliding Window Counter
13. GCRA (most important for distributed systems)
14. Adaptive Limiter
15. Composite Limiter

**Phase 2 — Resilience Patterns (tickets T-201 to T-206):**
16. Circuit Breaker (most complex — build state machine, then metrics window, then Execute)
17. Circuit Breaker Registry
18. Bulkhead (semaphore + thread pool)
19. Retry + all 5 backoff strategies
20. Timeout + Fallback + Hedge
21. Pipeline builder

**Phase 3 — Distributed + Middleware (tickets T-301 to T-304):**
22. Redis Store
23. Distributed implementations for all algorithms
24. HTTP middleware (rate limit + CB) with full response headers
25. gRPC interceptors

**Phase 4 — Demo Server (tickets T-401 to T-406):**
26. Server scaffolding + router + middleware
27. Rate limiter handlers
28. Circuit breaker handlers
29. WebSocket hub + real-time streaming
30. Load simulation engine
31. Prometheus metrics exporter

**Phase 5 — Next.js Frontend (tickets T-501 to T-509):**
32. Project setup + design system + API client + WS manager
33. App layout + sidebar + shared components
34. Overview page
35. All 6 algorithm deep-dive pages with animations
36. Circuit breaker state machine page
37. Load simulator page
38. Comparison + Pipeline + Docs pages

**Phase 6 — Production Hardening (tickets T-601 to T-605):**
39. Structured logging with slog + mandatory fields
40. OpenTelemetry tracing
41. Security headers + input validation
42. Goroutine leak detection in all tests
43. Memory safety chaos tests

**Phase 7 — Infra + Docs (tickets T-701 to T-706):**
44. Dockerfile (multi-stage, distroless) + docker-compose (with Redis, Prometheus, Grafana)
45. GitHub Actions CI + release workflows
46. Kubernetes manifests
47. Complete README.md
48. godoc polish pass
49. Examples directory

**Phase 8 — Final Verification (tickets T-801 to T-803):**
50. Full test suite: `make test-race` + `make fuzz-long` + `make bench` + `make verify-deps`
51. End-to-end smoke test
52. Release checklist + tag v1.0.0

---

## ACCEPTANCE CRITERIA

The project is **done** when every single item below is checked. No partial credit.

### Library — Correctness
- [ ] `go test ./...` — zero failures
- [ ] `go test -race -count=3 ./...` — zero races across 3 runs (not just once)
- [ ] `go test -fuzz=Fuzz -fuzztime=5m ./ratelimit/tokenbucket/` — no panics, no invariant violations
- [ ] `go test -fuzz=Fuzz -fuzztime=5m ./ratelimit/gcra/` — no panics
- [ ] `go test -fuzz=Fuzz -fuzztime=5m ./circuitbreaker/` — no panics, state never undefined
- [ ] Test coverage ≥ 85% across all core packages (`go tool cover`)
- [ ] `make verify-deps` — zero external deps in core library
- [ ] Token bucket: `allow(10)` at 10/s, deny 11th, allow after exactly `1/rate` duration (using ManualClock)
- [ ] Token bucket: `AllowN(11)` when capacity=10 always returns Allowed=false (over-capacity)
- [ ] GCRA: TAT calculation verified against 3 known-good test vectors from the ATM Forum spec
- [ ] Sliding window log vs counter: same load, counter within 10% of log result
- [ ] Fixed window: boundary burst test documents and demonstrates 2x limit possibility
- [ ] Circuit breaker: all 4 state transitions triggered and verified with ManualClock
- [ ] Circuit breaker: context cancellation does NOT increment failure count
- [ ] Composite AND: token NOT consumed from limiter A when limiter B denies
- [ ] `testutil.LeakChecker` passes on all goroutine-spawning types after `Close()`
- [ ] Chaos test (1000 goroutines, 100k ops): invariants hold for all algorithms

### Library — Performance (all must be met — benchmark, don't guess)
- [ ] `BenchmarkTokenBucket_Allow`: < 200 ns/op
- [ ] `BenchmarkTokenBucket_Allow_Parallel`: < 500 ns/op (GOMAXPROCS goroutines)
- [ ] `BenchmarkFixedWindow_Allow`: < 150 ns/op
- [ ] `BenchmarkGCRA_Allow`: < 200 ns/op
- [ ] `BenchmarkSlidingWindowCounter_Allow`: < 200 ns/op
- [ ] `BenchmarkSlidingWindowLog_Allow`: < 1 µs/op (acceptable — O(n) expected)
- [ ] `BenchmarkCircuitBreaker_Execute_Closed`: < 300 ns/op
- [ ] `BenchmarkCircuitBreaker_Execute_Open`: < 100 ns/op (fast reject)
- [ ] `BenchmarkCircuitBreaker_Execute_Closed_Parallel`: < 500 ns/op

### Library — API & Documentation Quality
- [ ] `godoc -http=:6060` — every package, type, function, method has a doc comment
- [ ] Every algorithm package has a package-level comment with: algorithm description, time/space complexity, reference link (RFC or paper)
- [ ] `Example_*` functions present for all 7 algorithms + circuit breaker + pipeline
- [ ] All errors: `errors.Is(err, ErrLimitExceeded)` works through 3 levels of wrapping
- [ ] All errors: `errors.As(err, &RateLimitError{})` extracts RetryAfter correctly
- [ ] Every exported type documented as "safe for concurrent use"
- [ ] `go vet ./...` — zero warnings
- [ ] `golangci-lint run` — zero issues

### HTTP Middleware
- [ ] Rate limit middleware sets all 5 headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `X-RateLimit-Policy`, `Retry-After` (on 429)
- [ ] `KeyByIP()` correctly extracts IP from `X-Forwarded-For` → `X-Real-IP` → `RemoteAddr` in that priority
- [ ] 429 response body is valid JSON: `{"error": "rate_limit_exceeded", "retry_after": 1.5, "limit": 100}`
- [ ] `SkipFunc` allows bypassing rate limiting for specific routes
- [ ] gRPC interceptor returns `codes.ResourceExhausted` on limit

### Distributed (Redis)
- [ ] Integration test: 5 independent limiters, shared Redis, global limit respected (not 5x)
- [ ] Redis unavailability: fallback to in-memory (configurable) — does not crash
- [ ] Redis Lua scripts for token bucket and GCRA are atomic (verified by running 1000 concurrent clients)

### Demo Server
- [ ] `curl http://localhost:8080/health/live` → 200 always
- [ ] `curl http://localhost:8080/health/ready` → 503 during startup, 200 after
- [ ] All 7 rate limiter `/allow` endpoints return correct Result JSON with all fields
- [ ] All `/configure` endpoints update limiter behavior immediately (visible in next request)
- [ ] Circuit breaker `/force-open` transitions state, `/execute` after returns 503 with circuit error
- [ ] WebSocket `/ws/v1/limiters/{algo}` streams state every 100ms — verify with wscat
- [ ] Simulation: send 1000 req/s burst to 100 req/s limit — WS streams show 90% denied
- [ ] `curl http://localhost:8080/metrics | grep resilience_ratelimit` — all metrics present
- [ ] Security headers present on all responses: `X-Content-Type-Options`, `X-Frame-Options`
- [ ] Empty `key` param returns 400 with JSON error (not 500)
- [ ] SIGTERM: in-flight requests complete, server exits cleanly within 20s

### Frontend
- [ ] All 7 algorithm pages load with live data, no skeleton stuck in loading state
- [ ] Token bucket animation: water level moves in real time, drip animation visible
- [ ] Circuit breaker state machine diagram pulses in the active state color
- [ ] State transition triggers animation: arrow lights up when state changes via WS event
- [ ] Simulator: 1000 req/s burst test — scatter plot updates in real time, no browser freeze
- [ ] Fixed vs sliding window comparison: boundary burst difference visible and labeled
- [ ] All pages: loading skeleton matches content shape
- [ ] All pages: error state shows with retry button (test by stopping backend)
- [ ] WebSocket disconnect banner appears within 2s of backend stopping, clears on reconnect
- [ ] `npm run check` — zero TypeScript errors
- [ ] `npm run lint` — zero lint errors
- [ ] `npm run build` — no build errors, bundle < 300KB gzipped total
- [ ] All interactive elements keyboard-accessible with visible focus ring
- [ ] All charts have "View as table" button for screen reader accessibility

### Operations
- [ ] `make build` — single binary < 30MB, serves frontend at `http://localhost:8080`
- [ ] `docker-compose up` — full stack healthy within 30s
- [ ] Grafana `http://localhost:3001` — pre-loaded dashboard shows real metrics within 60s
- [ ] `docker-compose up` then SIGTERM → `docker-compose down` — clean shutdown, no "zombie" containers
- [ ] GitHub Actions CI passes on clean branch (test, race, fuzz, lint, verify-deps, build)
- [ ] Release workflow: `git tag v1.0.0 && git push --tags` triggers Docker push + GitHub Release
- [ ] `go list -m github.com/sanskarpan/resilience@v1.0.0` resolves on proxy.golang.org