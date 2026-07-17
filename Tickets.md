# Tickets — Rate Limiter + Circuit Breaker Library
## Phase-Wise Build Plan for Claude Code

> **How to use this file with Claude Code:**
> Feed one phase at a time. Start every session with:
> _"Read tickets.md. Complete all tickets in Phase N. Do not start Phase N+1 until every acceptance criterion in Phase N is checked. Run the specified verification command before marking a ticket done."_
>
> **Ticket format:**
> - `[P]` = Priority: `[P0]` blocker, `[P1]` required, `[P2]` nice-to-have
> - Each ticket is self-contained with: goal, files to create/modify, implementation notes, and a verification command
> - Tickets within a phase can be parallelized unless marked `[DEPENDS ON: T-XXX]`

---

## Phase 0 — Foundation & Scaffolding
> Goal: Repo skeleton, shared internals, build tooling. Nothing domain-specific yet.
> Verification gate: `go build ./...` passes, `make dev` starts without errors.

---

### T-001 [P0] — Initialize Go module and repository structure

**Goal:** Create the full directory skeleton, go.mod, and empty placeholder files so all import paths resolve.

**Files to create:**
```
go.mod                          # module github.com/sanskarpan/resilience, go 1.23
go.sum                          # empty initially
.gitignore
.golangci.yml                   # linter config (see spec)
CHANGELOG.md                    # start with ## [Unreleased]
LICENSE                         # MIT
README.md                       # skeleton — full content added in Phase 7
```

**go.mod must have:**
- Module path: `github.com/sanskarpan/resilience`
- Go version: `1.23`
- Zero `require` entries in the core module (zero deps rule)

**`.golangci.yml` must enable:**
```yaml
linters:
  enable:
    - gofmt
    - govet
    - errcheck
    - staticcheck
    - gosimple
    - ineffassign
    - unused
    - misspell
    - godot          # godoc comment punctuation
    - godox          # catch TODO/FIXME/HACK in prod code
    - exhaustive     # exhaustive switch on enums
    - wrapcheck      # errors must be wrapped
    - cyclop         # cyclomatic complexity limit 10
    - gocognit       # cognitive complexity limit 15
    - funlen         # function length limit 80 lines
    - maintidx       # maintainability index
linters-settings:
  cyclop:
    max-complexity: 10
  funlen:
    lines: 80
    statements: 50
```

**Verification:** `go mod tidy && go build ./...` — must produce zero errors.

---

### T-002 [P0] — Shared internal packages: Clock, errors, and atomic helpers

**Goal:** Build the foundation types that every algorithm depends on. These live in `internal/` — not exported, only used by library packages.

**Files to create:**
```
internal/clock/clock.go         # Clock interface + RealClock + ManualClock
internal/clock/clock_test.go    # ManualClock advance/sleep/timer tests
internal/atomicx/float64.go     # atomic float64 using sync/atomic + math.Float64bits
internal/atomicx/float64_test.go
```

**`internal/clock/clock.go` spec:**
```go
// Package clock provides a mockable time source for deterministic testing.
// All time-dependent components accept a Clock interface rather than calling
// time.Now() directly. This is the most important architectural decision
// in this library — it makes every algorithm fully testable without time.Sleep.
package clock

type Clock interface {
    Now() time.Time
    Sleep(d time.Duration)
    Since(t time.Time) time.Duration
    Until(t time.Time) time.Duration
    NewTimer(d time.Duration) Timer
    NewTicker(d time.Duration) Ticker
    AfterFunc(d time.Duration, f func()) Timer
}

type Timer interface {
    C() <-chan time.Time
    Stop() bool
    Reset(d time.Duration) bool
}

type Ticker interface {
    C() <-chan time.Time
    Stop()
    Reset(d time.Duration)
}

// RealClock is the production implementation using the stdlib time package.
type RealClock struct{}

// ManualClock is a test double whose time only advances when Advance() is called.
// All goroutines blocked on Sleep or timers are unblocked deterministically.
type ManualClock struct {
    mu      sync.Mutex
    now     time.Time
    timers  []*manualTimer  // sorted by fire time
}

func (c *ManualClock) Advance(d time.Duration)
// Advance moves the clock forward by d. All timers/sleeps that would have
// fired in the interval [old_now, old_now+d] fire in order. This method
// is safe for concurrent use.
```

**`internal/atomicx/float64.go` spec:**
```go
// AtomicFloat64 provides atomic load/store/add for float64 values.
// Uses math.Float64bits / math.Float64frombits with sync/atomic underneath.
// This avoids a mutex on the hot path for token count in TokenBucket.
type AtomicFloat64 struct{ v uint64 }

func (a *AtomicFloat64) Load() float64
func (a *AtomicFloat64) Store(f float64)
func (a *AtomicFloat64) Add(delta float64) float64  // returns new value
func (a *AtomicFloat64) CompareAndSwap(old, new float64) bool
```

**Verification:**
```bash
go test ./internal/... -race -count=3
# ManualClock: verify Advance(500ms) fires a 100ms timer 5 times in order
# AtomicFloat64: verify 1000 concurrent Add(1.0) produces exactly 1000.0
```

---

### T-003 [P0] — Core library interfaces and error types

**Goal:** Define the `Limiter` interface, `Result`, `State`, and all typed errors. This is the public API contract — everything else implements it.

**Files to create:**
```
ratelimit/limiter.go            # Limiter interface, Result, State, Options
ratelimit/errors.go             # typed errors: RateLimitError, ErrLimitExceeded, etc.
ratelimit/errors_test.go        # errors.Is / errors.As compatibility tests
```

**`ratelimit/limiter.go` — full spec:**
```go
// Package ratelimit provides rate limiting algorithms for Go applications.
// All implementations are safe for concurrent use. All time operations are
// performed through a Clock interface (see internal/clock) enabling
// deterministic testing without time.Sleep.
//
// Quick start:
//
//	limiter := tokenbucket.New(100, 10, tokenbucket.WithBurst(20))
//	result := limiter.Allow(ctx, "user:123")
//	if !result.Allowed {
//	    http.Error(w, "rate limited", http.StatusTooManyRequests)
//	    return
//	}
package ratelimit

type Limiter interface {
    Allow(ctx context.Context, key string) Result
    AllowN(ctx context.Context, key string, n int) Result
    Wait(ctx context.Context, key string) error
    WaitN(ctx context.Context, key string, n int) error
    Peek(ctx context.Context, key string) State
    Reset(ctx context.Context, key string) error
    Close() error
}

type Result struct {
    Allowed    bool
    Limit      int
    Remaining  int           // tokens/requests remaining after this call
    ResetAfter time.Duration // when the window/bucket fully resets
    RetryAfter time.Duration // minimum wait before this key will be allowed (0 if allowed)
    Algorithm  string        // e.g. "token_bucket", "gcra", "sliding_window_log"
    Metadata   Metadata      // algorithm-specific observability data
}

type Metadata map[string]any  // strongly typed at the algorithm level, any here for flexibility

type State struct {
    Key         string
    Algorithm   string
    Limit       int
    Remaining   int
    ResetAt     time.Time
    WindowStart time.Time
    Extra       map[string]any
}
```

**`ratelimit/errors.go` — full spec:**
```go
var (
    ErrLimitExceeded = errors.New("rate limit exceeded")
    ErrInvalidKey    = errors.New("invalid key: empty or contains illegal characters")
    ErrInvalidN      = errors.New("n must be >= 1")
    ErrClosed        = errors.New("limiter is closed")
    ErrContextDone   = errors.New("context cancelled while waiting for token")
)

type RateLimitError struct {
    Algorithm  string
    Key        string
    Limit      int
    RetryAfter time.Duration
    Err        error          // wraps ErrLimitExceeded
}

// Implement errors.Is for ErrLimitExceeded matching
// Implement errors.As for type assertion
// Implement Error() string with human-readable message
```

**Key validation rule:** keys must be non-empty strings, max 512 bytes, no null bytes. Validate in every `Allow()` call. Return `ErrInvalidKey` wrapped in `RateLimitError`.

**Verification:**
```bash
go test ./ratelimit/ -run TestErrors -v
# Must verify: errors.Is(err, ErrLimitExceeded) works when wrapped 3 levels deep
# Must verify: errors.As extracts RateLimitError.RetryAfter correctly
```

---

### T-004 [P0] — In-memory store and Store interface

**Goal:** Define the storage abstraction used by distributed algorithms. Implement the in-memory version used in all local limiters.

**Files to create:**
```
ratelimit/store/store.go        # Store interface
ratelimit/store/memory.go       # in-memory implementation
ratelimit/store/memory_test.go  # concurrent read/write tests
```

**`store/store.go`:**
```go
// Store is the persistence interface for distributed rate limiters.
// Implementations must be safe for concurrent use.
type Store interface {
    // Get returns the value for key. Returns ("", ErrNotFound) if absent.
    Get(ctx context.Context, key string) (string, error)

    // Set stores value for key with TTL. Overwrites existing.
    Set(ctx context.Context, key string, value string, ttl time.Duration) error

    // SetNX stores value only if key does not exist. Returns true if set.
    SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)

    // GetSet atomically gets current value and sets new value. Returns old value.
    GetSet(ctx context.Context, key string, value string, ttl time.Duration) (string, error)

    // Increment atomically increments integer value by delta. Creates key with delta if absent.
    // Sets TTL only on creation (when previous value was 0 / key absent).
    IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)

    // Eval executes a Lua script atomically (required for Redis; in-memory runs in mutex).
    Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)

    // Del deletes one or more keys.
    Del(ctx context.Context, keys ...string) error

    // Ping checks connectivity.
    Ping(ctx context.Context) error

    // Close releases resources.
    Close() error
}

var ErrNotFound = errors.New("key not found")
```

**`store/memory.go` implementation notes:**
- Use `sync.Map` for the key→entry map (read-heavy workloads dominate)
- Each entry: `value string`, `expiresAt time.Time`
- Background goroutine evicts expired keys every 30s (configurable, stoppable via `Close()`)
- `Eval` for in-memory: parse the Lua script as a simple key→command mapping (limited Lua support needed for GCRA script). Use `gopher-lua` only if needed — otherwise implement the 3 specific scripts used (token bucket, GCRA, sliding window) as named Go functions.
- **No actual Lua interpreter needed** — the `Eval` in memory.go dispatches to registered script handlers by script hash

**Verification:**
```bash
go test ./ratelimit/store/ -race -count=5
# 1000 goroutines concurrent IncrBy on same key — final value must equal 1000
# TTL expiry: set key, advance time, verify Get returns ErrNotFound
# SetNX: concurrent calls — exactly one must return true
```

---

### T-005 [P0] — Makefile, dev tooling, pre-commit hooks

**Goal:** Developer experience tooling. Running `make dev` starts everything. Running `make test` runs everything.

**Files to create:**
```
Makefile
.pre-commit-config.yaml
tools.go                        # blank import of tools for go:generate
```

**Complete Makefile:**
```makefile
GOBIN := $(shell go env GOPATH)/bin
MODULE := github.com/sanskarpan/resilience
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags="-s -w -X $(MODULE)/server/version.Version=$(VERSION)"

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

dev: ## Start backend + frontend dev servers
	@command -v air >/dev/null || go install github.com/cosmtrek/air@latest
	@(cd server && air -c .air.toml) & (cd frontend && npm run dev) & wait

build: build-frontend build-go ## Build everything into single binary

build-frontend:
	cd frontend && npm ci --frozen-lockfile && npm run build

build-go: build-frontend
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/demo-server ./server/

test: ## Run all tests
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	cd frontend && npm run check && npm run lint

test-race: ## Run tests with race detector (required before every PR)
	go test -race -count=3 ./...

test-unit: ## Run unit tests only (fast, no integration)
	go test -short ./...

test-integration: ## Run integration tests (requires Docker)
	go test -tags=integration -run Integration ./...

fuzz: ## Run fuzz tests (30s each)
	@for pkg in tokenbucket gcra slidingwindow/log circuitbreaker; do \
		echo "Fuzzing $$pkg..."; \
		go test -fuzz=Fuzz -fuzztime=30s ./ratelimit/$$pkg/ 2>/dev/null || \
		go test -fuzz=Fuzz -fuzztime=30s ./$$pkg/ 2>/dev/null || true; \
	done

fuzz-long: ## Run fuzz tests for 5 minutes each (pre-release)
	@FUZZTIME=5m $(MAKE) fuzz FUZZTIME=5m

bench: ## Run all benchmarks
	go test -bench=. -benchmem -count=5 ./... | tee bench-$(VERSION).txt

bench-compare: ## Compare benchmarks: make bench-compare OLD=main NEW=HEAD
	@command -v benchstat >/dev/null || go install golang.org/x/perf/cmd/benchstat@latest
	git stash && git checkout $(OLD) && go test -bench=. -benchmem -count=10 ./... > /tmp/old.txt
	git checkout $(NEW) && git stash pop && go test -bench=. -benchmem -count=10 ./... > /tmp/new.txt
	benchstat /tmp/old.txt /tmp/new.txt

lint: ## Run all linters
	golangci-lint run --timeout 5m
	cd frontend && npm run lint

lint-fix: ## Auto-fix lint issues
	golangci-lint run --fix
	cd frontend && npm run lint -- --fix

godoc: ## Serve godoc locally
	@echo "Godoc available at http://localhost:6060/pkg/$(MODULE)/"
	godoc -http=:6060

verify-deps: ## Verify core library has zero external runtime dependencies
	@for pkg in ratelimit circuitbreaker bulkhead retry timeout fallback pipeline; do \
		DEPS=$$(go list -f '{{join .Imports "\n"}}' ./$$pkg/... 2>/dev/null | \
			grep -v "^$(MODULE)" | grep "\." | grep -v "^testing\b" || true); \
		if [ -n "$$DEPS" ]; then echo "❌ $$pkg has external deps: $$DEPS"; exit 1; fi; \
		echo "✓ $$pkg has zero external deps"; \
	done

docker: ## Build Docker image
	docker build -t resilience-demo:$(VERSION) .

docker-run: ## Start full stack with Docker Compose
	docker-compose up --build

clean: ## Remove build artifacts
	rm -rf bin/ frontend/.next frontend/out coverage.out bench-*.txt
```

**Verification:** `make help` shows all targets. `make verify-deps` passes on empty library.

---

## Phase 1 — Rate Limiting Algorithms (Core Library)
> Goal: All 7 rate limiting algorithms implemented, tested, benchmarked.
> Every algorithm must: implement the `Limiter` interface, use `Clock` interface, pass race detector, hit benchmark targets.
> Verification gate: `make test-race` passes. `make bench` shows all targets met.

---

### T-101 [P0] — Token Bucket algorithm

**Goal:** Implement the token bucket — the most fundamental rate limiting algorithm.

**Files to create:**
```
ratelimit/tokenbucket/tokenbucket.go
ratelimit/tokenbucket/options.go
ratelimit/tokenbucket/tokenbucket_test.go
ratelimit/tokenbucket/tokenbucket_bench_test.go
ratelimit/tokenbucket/example_test.go        # godoc Example functions
```

**Core implementation notes:**
- Store tokens as `atomicx.AtomicFloat64` for lock-free reads on the fast path
- Lazy refill: compute elapsed time since `lastRefill` on every `Allow()` — no background goroutine
- Lock acquisition: only `sync.Mutex` when refill calculation is needed (write path). Read path (check if tokens >= 1 after optimistic atomic read) can be lock-free.
- `AllowN` must be fully atomic — either consume n tokens or consume 0, never partial
- `Wait` must use `clock.NewTimer` (not `time.Sleep`) so it works with `ManualClock` in tests
- State cleanup: use `sync.Map` for per-key buckets, background goroutine evicts keys inactive for `idleTimeout` (configurable, default: 5 * window)

**Metadata to include in Result:**
```go
Metadata{
    "tokens_before":    float64,   // tokens before this request
    "tokens_after":     float64,   // tokens after this request (0 if denied)
    "refilled_amount":  float64,   // tokens added during lazy refill
    "time_to_full_ms":  int64,     // milliseconds until bucket is full
    "capacity":         float64,
    "refill_rate_per_s": float64,
}
```

**Required test cases:**
```go
TestTokenBucket_BasicAllow                    // allow up to capacity
TestTokenBucket_DenyWhenEmpty                 // deny when tokens exhausted
TestTokenBucket_RefillAfterWait               // advance clock, verify tokens refill
TestTokenBucket_AllowN_AtomicConsume          // AllowN consumes all or nothing
TestTokenBucket_AllowN_ExceedsCapacity        // AllowN(n > capacity) always fails
TestTokenBucket_Wait_RespectsContext          // cancel context, Wait returns error
TestTokenBucket_Wait_UnblocksAfterRefill      // Wait unblocks when tokens arrive
TestTokenBucket_BurstBehavior                 // burst up to capacity, then rate-limited
TestTokenBucket_Concurrent_NoRace             // 500 goroutines, -race must pass
TestTokenBucket_Concurrent_RemainingNonNeg    // invariant: remaining >= 0 always
TestTokenBucket_Close_StopsCleanup            // Close() stops background goroutine
TestTokenBucket_MultipleKeys_Isolation        // key "a" and key "b" are independent
TestTokenBucket_Reset_ClearsState             // Reset restores to full capacity
```

**Benchmark targets:**
```
BenchmarkTokenBucket_Allow_SingleKey        < 200 ns/op
BenchmarkTokenBucket_Allow_100Keys          < 250 ns/op
BenchmarkTokenBucket_AllowN_5              < 220 ns/op
BenchmarkTokenBucket_Allow_Parallel        < 500 ns/op  (GOMAXPROCS goroutines)
BenchmarkTokenBucket_Peek                  < 100 ns/op
```

**Fuzz test:**
```go
func FuzzTokenBucket(f *testing.F) {
    // Invariants to check:
    // 1. Result.Remaining always in [0, capacity]
    // 2. Never panics on any input
    // 3. After Allow() returns Allowed=true, tokens decreased by exactly n
}
```

**Verification:**
```bash
go test ./ratelimit/tokenbucket/ -race -v -count=3
go test ./ratelimit/tokenbucket/ -bench=. -benchmem -count=5
go test ./ratelimit/tokenbucket/ -fuzz=FuzzTokenBucket -fuzztime=60s
```

---

### T-102 [P0] — Leaky Bucket algorithm

**Files to create:**
```
ratelimit/leakybucket/leakybucket.go
ratelimit/leakybucket/options.go
ratelimit/leakybucket/leakybucket_test.go
ratelimit/leakybucket/leakybucket_bench_test.go
ratelimit/leakybucket/example_test.go
```

**Core implementation notes:**
- Queue implementation: buffered `chan token` where `type token struct{ key string; result chan Result }`
- Background leaker goroutine: reads from internal dispatch channel, ticks via `clock.NewTicker(1/rate)`
- Per-key queues: `sync.Map[string]*keyQueue` where each keyQueue has its own buffered channel
- `Allow()`: non-blocking send to key's channel. If full → immediate deny with `RetryAfter = (queueDepth / rate)`
- `Wait()`: blocking send to key's channel with context. Blocks until the leaker drains to this request.
- **Critical:** leaker goroutine exits cleanly on `Close()` via done channel. Test this explicitly.
- **Difference from token bucket documented in package godoc**: leaky bucket enforces constant output rate, smoothing bursts. Token bucket permits bursting up to capacity.

**Metadata:**
```go
Metadata{
    "queue_depth":       int,      // current queue occupancy
    "queue_capacity":    int,      // maximum queue size
    "leak_rate_per_s":  float64,  // configured leak rate
    "estimated_wait_ms": int64,   // estimated wait time if queued
}
```

**Required test cases:**
```go
TestLeakyBucket_ConstantOutputRate         // verify output at exactly leak_rate req/s
TestLeakyBucket_DenyWhenQueueFull          // capacity+1 concurrent sends → one denied
TestLeakyBucket_QueuedRequestsProcessed    // queued requests eventually succeed
TestLeakyBucket_Wait_ContextCancellation   // cancel while in queue → removed from queue
TestLeakyBucket_CloseStopsLeaker           // Close() drains and stops goroutine (no leak)
TestLeakyBucket_Concurrent_NoRace
```

**Verification:**
```bash
go test ./ratelimit/leakybucket/ -race -v
go test ./ratelimit/leakybucket/ -bench=. -benchmem
```

---

### T-103 [P0] — Fixed Window Counter

**Files to create:**
```
ratelimit/fixedwindow/fixedwindow.go
ratelimit/fixedwindow/options.go
ratelimit/fixedwindow/fixedwindow_test.go
ratelimit/fixedwindow/example_test.go
```

**Core implementation notes:**
- Window boundary computed as: `windowStart = time.Unix(0, (now.UnixNano()/window.Nanoseconds())*window.Nanoseconds())`
- Per-key counter: `sync.Map[string]*windowCounter` — only lock per-key, not global
- Window reset: compare stored `windowStart` to computed. If different, atomically reset counter.
- **Boundary burst test**: must include a test demonstrating 2x limit requests possible straddling a boundary. Document this in test comment as the known limitation.
- `RetryAfter`: `windowStart + window - now`

**Required test cases:**
```go
TestFixedWindow_AllowUpToLimit
TestFixedWindow_DenyBeyondLimit
TestFixedWindow_ResetAtWindowBoundary
TestFixedWindow_BoundaryBurstPossible          // DOCUMENTS THE KNOWN LIMITATION
TestFixedWindow_RetryAfterIsCorrect
TestFixedWindow_Concurrent_NoRace
```

**Benchmark target:** `BenchmarkFixedWindow_Allow < 150 ns/op`

---

### T-104 [P0] — Sliding Window Log

**Files to create:**
```
ratelimit/slidingwindow/log.go
ratelimit/slidingwindow/log_test.go
ratelimit/slidingwindow/log_bench_test.go
```

**Core implementation notes:**
- Per-key log: `sync.Map[string]*keyLog` with `sync.Mutex` and `[]time.Time` slice
- On `Allow(key)`: prune timestamps < `now - window`, check length, append now
- `RetryAfter`: `oldest_timestamp_in_window + window - now`
- Memory: expose `log_size` in Metadata — important for observability (shows memory cost)
- Cleanup goroutine: every `window` duration, iterate all keys, delete keys with empty logs
- **Memory complexity documented**: O(requests_per_window) — exact but expensive. Documented clearly in package godoc with comparison to counter approach.

**Required test cases:**
```go
TestSlidingWindowLog_ExactBoundary          // request at exact window boundary is counted
TestSlidingWindowLog_OldRequestsExpire      // requests older than window don't count
TestSlidingWindowLog_RetryAfterPrecise      // RetryAfter = time until oldest request expires
TestSlidingWindowLog_MemoryCleanup          // inactive keys evicted by cleanup goroutine
TestSlidingWindowLog_Concurrent_NoRace
```

**Benchmark target:** `BenchmarkSlidingWindowLog_Allow < 1 µs/op` (acceptable — O(n) is expected)

---

### T-105 [P0] — Sliding Window Counter

**Files to create:**
```
ratelimit/slidingwindow/counter.go
ratelimit/slidingwindow/counter_test.go
ratelimit/slidingwindow/counter_bench_test.go
ratelimit/slidingwindow/example_test.go      # covers both log and counter
```

**Core implementation notes:**
- Two buckets per key: `current` and `previous`, each `{count int64, windowStart time.Time}`
- Formula: `effectiveCount = float64(previous.count) * (1.0 - elapsed/window) + float64(current.count)`
- Shift: when `now >= current.windowStart + window` → `previous = current`, reset `current`
- Use `sync.RWMutex` per key — reads (Peek) don't need write lock
- **Approximation error**: document in godoc that max error is `limit * 1/window` requests at boundary. Expose `approximation_error_bound` in Metadata.

**Metadata:**
```go
Metadata{
    "current_count":          int64,
    "previous_count":         int64,
    "effective_count":        float64,
    "elapsed_fraction":       float64,   // what fraction of current window has elapsed
    "approximation_method":   "sliding_window_counter",
    "max_error_bound":        float64,   // worst-case approximation error
}
```

**Required test cases:**
```go
TestSlidingWindowCounter_BasicAllow
TestSlidingWindowCounter_ApproximationFormula    // verify formula with known inputs
TestSlidingWindowCounter_NoBoundaryBurst         // improved over fixed window
TestSlidingWindowCounter_WindowShift             // verify prev/current swap logic
TestSlidingWindowCounter_Concurrent_NoRace
// COMPARISON TEST: run same load against Log and Counter variants, verify Counter approx within 10%
TestSlidingWindowComparison_LogVsCounter
```

**Benchmark target:** `BenchmarkSlidingWindowCounter_Allow < 200 ns/op`

---

### T-106 [P0] — GCRA (Generic Cell Rate Algorithm)

**Goal:** Implement GCRA — the most efficient and Redis-friendly rate limiting algorithm.

**Files to create:**
```
ratelimit/gcra/gcra.go
ratelimit/gcra/options.go
ratelimit/gcra/gcra_test.go
ratelimit/gcra/gcra_bench_test.go
ratelimit/gcra/example_test.go
```

**Core implementation — exact formula:**
```
emissionInterval = window / limit           // e.g. 100ms for 10 req/s
burstOffset      = emissionInterval * (burst - 1)
                                            // allows burst-1 additional requests
TAT              = max(lastTAT[key], now) + emissionInterval
allowed          = TAT - burstOffset <= now + tolerance  // tolerance = 0 normally
RetryAfter       = TAT - burstOffset - now  (when denied)
Remaining        = floor((now + burstOffset - TAT) / emissionInterval)
```

**Implementation notes:**
- Per-key TAT stored as `sync.Map[string]time.Time`
- **No floating point** in the critical path — use `time.Duration` arithmetic only (integer nanoseconds)
- `burstOffset` is computed once at construction and stored as `time.Duration`
- When key absent: treat `lastTAT` as `time.Time{}` which `max(zero, now)` evaluates to `now`

**Why GCRA is special — must appear in package godoc:**
```
GCRA is used by Stripe, Shopify, and many high-performance APIs because:
1. A single timestamp per key (not a counter + window, not N timestamps)
2. Mathematically exact — no approximation unlike sliding window counter
3. Perfectly suited for distributed systems (Redis SET on single key, CAS operation)
4. Allows precise burst control via burstOffset
Reference: "Traffic Management" in ATM Forum specification; also see
https://brandur.org/rate-limiting for a practical implementation guide.
```

**Required test cases:**
```go
TestGCRA_BasicRate
TestGCRA_BurstAllowed                       // burst=5 allows 5 initial requests
TestGCRA_BurstExhausted                     // 6th request in burst is denied
TestGCRA_ExactTATCalculation                // verify TAT formula with known values
TestGCRA_RetryAfterPrecise                  // RetryAfter = exact time to next allowed
TestGCRA_RemainingCalculation               // verify Remaining formula
TestGCRA_KeyAbsent_BehavesCorrectly         // first request always allowed
TestGCRA_Concurrent_NoRace
TestGCRA_NoFloatDrift                       // 10M iterations, verify no drift in timing
```

**Benchmark target:** `BenchmarkGCRA_Allow < 200 ns/op` (must beat sliding window log)

**Fuzz test:**
```go
func FuzzGCRA(f *testing.F) {
    // Invariants:
    // 1. Remaining always in [0, burst]
    // 2. If Allowed=true, TAT advanced by exactly emissionInterval
    // 3. If Allowed=false, TAT unchanged
    // 4. Never panics
}
```

---

### T-107 [P1] — Adaptive Rate Limiter

**Files to create:**
```
ratelimit/adaptive/adaptive.go
ratelimit/adaptive/signals.go       # SignalSource interface + implementations
ratelimit/adaptive/adaptive_test.go
ratelimit/adaptive/example_test.go
```

**`signals.go` — SignalSource implementations:**
```go
type SignalSource interface {
    CPUPercent() float64      // 0-100
    ErrorRate() float64       // 0.0-1.0 (EMA of recent request outcomes)
    P99Latency() time.Duration
}

// RuntimeSignals uses Go runtime metrics for CPU proxy and a sliding EMA
// for error rate. P99 latency is tracked via an internal histogram.
type RuntimeSignals struct {
    errorRate  ewma         // exponential weighted moving average, α=0.1
    latencyHist histogram   // power-of-2 bucketed, 1µs to 10s
    mu         sync.Mutex
}

func (r *RuntimeSignals) RecordSuccess(latency time.Duration)
func (r *RuntimeSignals) RecordError(latency time.Duration)
// These must be called by the wrapping middleware/application after each request
```

**Adjustment algorithm:**
```
every adjustInterval (default 1s):
  score = weighted_score(cpu, errorRate, p99Latency)
  if score > 0.8:  // system stressed
      target = current * 0.9
  elif score > 0.6:
      target = current * 0.95
  elif score < 0.3:  // healthy
      target = current * 1.05
  elif score < 0.2:
      target = current * 1.1
  
  // Gradient smoothing prevents oscillation
  new_limit = int(float64(current)*0.7 + float64(target)*0.3)
  new_limit = clamp(new_limit, min, max)
```

**Verification:**
```bash
go test ./ratelimit/adaptive/ -race -v
# Test: signal error rate 0.8 → limit decreases within 3 adjustment cycles
# Test: signal CPU 20% → limit increases within 3 adjustment cycles
# Test: gradient smoothing — no oscillation between min and max
```

---

### T-108 [P1] — Composite Limiter

**Files to create:**
```
ratelimit/composite/composite.go
ratelimit/composite/composite_test.go
ratelimit/composite/example_test.go
```

**Implementation notes:**
- `AND` mode: calls all limiters in parallel using goroutines, waits for all. If any denies, returns the most restrictive Result (smallest Remaining, longest RetryAfter). Tokens from allowing limiters are NOT consumed if any limiter denies — requires two-phase: check then consume. Implement as: check all, if any deny → return deny without consuming. If all allow → consume from all.
- `OR` mode: first Allow response wins. Cancel other goroutines via context.
- **Two-phase allow for AND mode is critical** — without it, a request could consume a token from limiter A even when limiter B will deny it.

**Verification:**
```bash
go test ./ratelimit/composite/ -race -v
# Test: AND mode — A allows, B denies → overall denied, A's token NOT consumed
# Test: AND mode — both allow → both tokens consumed
# Test: OR mode — A denies, B allows → overall allowed
```

---

## Phase 2 — Circuit Breaker & Resilience Patterns
> Goal: Circuit breaker with full FSM, all supporting patterns (bulkhead, retry, fallback, pipeline).
> Verification gate: `make test-race` passes. Circuit breaker state machine is deterministic with ManualClock.

---

### T-201 [P0] — Circuit Breaker core FSM

**Goal:** Implement the circuit breaker — the most complex component in this library.

**Files to create:**
```
circuitbreaker/circuitbreaker.go
circuitbreaker/config.go
circuitbreaker/state.go
circuitbreaker/metrics.go           # internal window-based failure tracking
circuitbreaker/errors.go
circuitbreaker/circuitbreaker_test.go
circuitbreaker/circuitbreaker_bench_test.go
circuitbreaker/example_test.go
```

**State machine rules — implement exactly:**
```
Closed → Open:
  COUNT_BASED: failures in ring buffer[last N] >= FailureThreshold
  TIME_BASED:  failures in rolling window >= FailureThreshold AND
               total_requests >= MinimumRequests AND
               failure_rate >= FailureRateThreshold

Open → HalfOpen:
  clock.Now() >= openedAt + OpenTimeout   (lazy — triggered on next Execute() call)
  Uses atomic CAS to prevent race between concurrent triggers

HalfOpen → Closed:
  consecutive successes >= SuccessThreshold
  Reset ALL metrics

HalfOpen → Open:
  any single failure
  openedAt = now (reset full timeout)

CONCURRENT HALF-OPEN PROBES:
  atomic counter tracks inflight probes in HalfOpen
  if inflight >= HalfOpenMaxRequests: REJECT (not fail — caller should not retry)
```

**`circuitbreaker/metrics.go` — two window types:**
```go
type WindowType int
const (
    CountBased WindowType = iota  // ring buffer of last N outcomes
    TimeBased                     // fixed-width time buckets
)

// CountWindow: ring buffer of Outcome{Success, Failure}
// O(1) insert, O(1) query via precomputed failure count
type countWindow struct {
    ring    []outcome    // circular buffer
    head    int          // write index
    size    int          // configured size
    failures int         // precomputed — updated on every write
    total   int          // min(writes, size)
    mu      sync.Mutex
}

// TimeWindow: slice of buckets covering [now-windowDuration, now]
// Each bucket: 1-second wide (configurable). Slide forward as time passes.
type timeWindow struct {
    buckets     []timeBucket
    bucketWidth time.Duration
    total       int     // total buckets
    failures    int64   // precomputed total failures across buckets
    requests    int64   // precomputed total requests across buckets
    mu          sync.Mutex
    clock       Clock
}
```

**`Execute()` implementation — must handle:**
1. State check (Open → reject with `ErrCircuitOpen`)
2. HalfOpen probe limit check (over limit → reject)
3. Optional request timeout via `config.RequestTimeout`
4. Call `fn(ctx)`
5. Record outcome (Success or Failure based on error type + `IsFailure` predicate)
6. Check thresholds and transition state if needed
7. Invoke appropriate callback
8. Return error (nil, fn's error, or circuit error)

**Error classification:**
```go
// IsFailure determines if an error should count as a circuit breaker failure.
// By default, any non-nil error counts. Custom classification:
type Config struct {
    IsFailure func(err error) bool  // nil = all errors count as failures
}
// Context cancellation should NOT count as a CB failure — caller cancelled.
// Timeout from CB's own RequestTimeout DOES count as a CB failure.
```

**Required test cases:**
```go
TestCB_InitialState_Closed
TestCB_OpenAfterFailureThreshold_CountBased
TestCB_OpenAfterFailureRateThreshold_TimeBased
TestCB_MinimumRequestsNotMet_StaysClosed          // 3 failures but MinReqs=10 → stays closed
TestCB_OpenToHalfOpen_AfterTimeout
TestCB_HalfOpen_ProbeSucceeds_CloseCircuit
TestCB_HalfOpen_ProbeFails_ReopenCircuit
TestCB_HalfOpen_ExcessProbesRejected              // inflight > MaxHalfOpenRequests → reject
TestCB_ContextCancellation_NotCountedAsFailure
TestCB_CustomIsFailure_FiltersErrors
TestCB_Callbacks_AllFiredCorrectly
TestCB_Execute_ReturnsCircuitOpenError            // errors.Is(err, ErrCircuitOpen)
TestCB_ExecuteWithFallback_FallbackCalledWhenOpen
TestCB_ExecuteWithFallback_FallbackCalledOnError
TestCB_Concurrent_StateTransitions_NoRace         // 1000 goroutines, rapid state changes
TestCB_MetricWindow_CountBased_CorrectCount
TestCB_MetricWindow_TimeBased_SlideCorrectly
TestCB_RequestTimeout_CountsAsFailure
```

**Benchmark targets:**
```
BenchmarkCB_Execute_Closed            < 300 ns/op
BenchmarkCB_Execute_Open              < 100 ns/op   (fast reject)
BenchmarkCB_Execute_Closed_Parallel   < 500 ns/op   (1000 goroutines)
```

---

### T-202 [P0] — Circuit Breaker Registry

**Files to create:**
```
circuitbreaker/registry.go
circuitbreaker/registry_test.go
```

```go
type Registry struct {
    breakers sync.Map  // name → *CircuitBreaker
    mu       sync.Mutex
}

var Global = NewRegistry()

// GetOrCreate returns existing CB or creates with given config.
// The config is only used on creation — subsequent calls with same name
// return the existing CB regardless of config argument.
func (r *Registry) GetOrCreate(name string, cfg Config) *CircuitBreaker

// Snapshot returns a point-in-time snapshot of all registered breakers.
func (r *Registry) Snapshot() map[string]Snapshot

type Snapshot struct {
    Name         string
    State        State
    Failures      int
    Successes    int
    Requests     int
    FailureRate  float64
    OpenedAt     time.Time    // zero if closed
    TimeUntilHalfOpen time.Duration  // 0 if not open
}
```

---

### T-203 [P0] — Bulkhead (concurrency limiter)

**Files to create:**
```
bulkhead/bulkhead.go
bulkhead/threadpool.go
bulkhead/bulkhead_test.go
bulkhead/example_test.go
```

**Semaphore bulkhead:**
```go
type Bulkhead struct {
    sem      chan struct{}
    maxWait  time.Duration
    inflight atomic.Int64
    rejected atomic.Int64   // for metrics
}

// Execute acquires a slot, runs fn, releases slot.
// If maxWait=0: non-blocking (reject immediately if full)
// If maxWait>0: wait up to maxWait for a slot
// Returns ErrBulkheadFull if no slot available
```

**Thread Pool bulkhead** — different use case: submit async tasks:
```go
type ThreadPool struct {
    workers  int
    queue    chan Task
    results  map[string]chan error    // task ID → result channel
    done     chan struct{}
    wg       sync.WaitGroup
}

type Task struct {
    ID  string
    Fn  func(ctx context.Context) error
}

func (tp *ThreadPool) Submit(ctx context.Context, task Task) (<-chan error, error)
// Returns channel that receives the result. ErrQueueFull if queue at capacity.
```

**Required tests:**
```go
TestBulkhead_AllowUpToConcurrencyLimit
TestBulkhead_RejectWhenFull_NoWait
TestBulkhead_QueueWithWait
TestBulkhead_ContextCancellationWhileWaiting
TestBulkhead_SlotReleasedAfterPanic        // defer guarantees release even on panic
TestBulkhead_MetricsAccurate              // inflight/rejected counts are correct
TestThreadPool_AsyncExecution
TestThreadPool_QueueFull_Rejected
```

---

### T-204 [P0] — Retry with all backoff strategies

**Files to create:**
```
retry/retry.go
retry/backoff/constant.go
retry/backoff/exponential.go
retry/backoff/jitter.go         # full jitter + equal jitter
retry/backoff/decorrelated.go   # decorrelated jitter (AWS paper)
retry/backoff/backoff.go        # BackoffStrategy interface
retry/retry_test.go
retry/backoff/backoff_test.go
retry/example_test.go
```

**Backoff interface:**
```go
type BackoffStrategy interface {
    // Next returns the delay before attempt number n (0-indexed).
    // n=0 is the first retry (after first failure).
    Next(attempt int) time.Duration
}
```

**Decorrelated jitter formula** (from AWS "Exponential Backoff And Jitter" blog post):
```
sleep = min(cap, random_between(base, previous_sleep * 3))
```

**Retry policy:**
```go
type Policy struct {
    MaxAttempts  int            // total attempts (1 = no retry)
    Backoff      BackoffStrategy
    RetryIf      func(err error) bool   // nil = retry all errors
    OnRetry      func(attempt int, err error, nextWait time.Duration)
    MaxDelay     time.Duration          // cap any backoff at this value
    Clock        Clock                  // for testing
}
```

**Required tests:**
```go
TestRetry_NoRetryOnSuccess
TestRetry_RetryUpToMaxAttempts
TestRetry_RetryIfPredicate
TestRetry_ContextCancellation_StopsRetrying
TestRetry_BackoffTimingsCorrect             // verify delays using ManualClock
TestRetry_ExponentialBackoff_Formula
TestRetry_FullJitter_Distribution           // statistical test: mean ≈ cap/2
TestRetry_DecorrelatedJitter_NoBoundExplosion // never exceeds cap
TestRetry_DoWithResult_TypeSafety
```

---

### T-205 [P1] — Timeout, Fallback, and Hedge

**Files to create:**
```
timeout/timeout.go
timeout/timeout_test.go
fallback/fallback.go
fallback/hedge.go
fallback/fallback_test.go
```

**Hedge request implementation notes:**
- Fire primary request
- After `hedgeDelay`, if primary not done, fire backup
- Select: first response wins, cancel the loser
- Track which response won (primary or backup) — expose in metadata
- `hedgeDelay` should be set to P95 latency of the target service — reduces P99 at cost of ~5% extra requests

---

### T-206 [P0] — Resilience Pipeline builder

**Files to create:**
```
pipeline/pipeline.go
pipeline/builder.go
pipeline/pipeline_test.go
pipeline/example_test.go
```

**Builder pattern:**
```go
p := pipeline.New().
    RateLimit(limiter, pipeline.KeyByIP()).
    Bulkhead(50, pipeline.WithMaxWait(10*time.Millisecond)).
    Timeout(5 * time.Second).
    CircuitBreaker(cb).
    Retry(retryPolicy).
    Build()

err := p.Execute(ctx, func(ctx context.Context) error {
    return callDownstream(ctx)
})
```

**Stage ordering is fixed and non-configurable** — this is intentional. The order Rate→Bulkhead→Timeout→CB→Retry is the correct production order for a reason (document each reason in godoc):
1. Rate limit first: don't waste a bulkhead slot on a request you'll deny anyway
2. Bulkhead before timeout: don't start timeout countdown while waiting for a worker
3. Timeout before CB: CB should see real failures, not timeouts from slow queue drain
4. CB before retry: don't retry if circuit is open
5. Retry innermost: only retry the actual call, not the whole pipeline

---

## Phase 3 — Distributed Support & HTTP/gRPC Middleware
> Goal: Redis-backed distributed implementations of all algorithms. HTTP and gRPC middleware.
> Verification gate: Redis integration tests pass with testcontainers.

---

### T-301 [P0] — Redis Store implementation

**Files to create:**
```
ratelimit/store/redis.go
ratelimit/store/redis_test.go        # integration test (build tag: integration)
```

**Implementation notes:**
- Use `go-redis/v9` — only in this file, not in core library
- Separate `go.mod` NOT needed — Redis is in a sub-package but same module. Use build tag `// go:build integration` on tests.
- `Eval` implementation: execute Lua scripts via `go-redis` client's `Eval` command
- Connection pool: configurable min/max idle, connection timeout, read/write timeout
- Retry on connection failure: 3 retries with exponential backoff on transient network errors
- **Fallback**: when Redis is unavailable (all retries exhausted), delegate to provided fallback `Store`. Default fallback: in-memory (allows traffic through). Configurable to `DenyAll` fallback.

**Required Lua scripts (implemented as Go string constants):**
```go
// TokenBucketScript: atomically read tokens + lastRefill, compute refill, check, decrement
const tokenBucketLuaScript = `...`

// GCRAScript: atomically read TAT, compute new TAT, check, store
const gcraLuaScript = `...`

// SlidingWindowLogScript: ZADD + ZCOUNT + EXPIRE in one script
const slidingWindowLogLuaScript = `...`
```

---

### T-302 [P0] — Distributed implementations for all algorithms

**Files to create:**
```
ratelimit/tokenbucket/distributed.go
ratelimit/fixedwindow/distributed.go
ratelimit/slidingwindow/distributed_log.go
ratelimit/slidingwindow/distributed_counter.go
ratelimit/gcra/distributed.go
```

**Key naming convention for Redis keys:** `{prefix}:{algorithm}:{key}` — e.g., `rl:tokenbucket:user:123`

**Integration tests using testcontainers:**
```go
//go:build integration

func TestDistributed_TokenBucket_GlobalLimit(t *testing.T) {
    // Start Redis container
    // Create 5 independent limiters pointing to same Redis
    // Send 100 concurrent requests across all 5 (limit=50)
    // Verify exactly 50 allowed, 50 denied
    // Verify no individual limiter allowed more than limit
}
```

---

### T-303 [P0] — HTTP Middleware

**Files to create:**
```
ratelimit/middleware/http.go
ratelimit/middleware/options.go
ratelimit/middleware/http_test.go
ratelimit/middleware/example_test.go
circuitbreaker/middleware/http.go
circuitbreaker/middleware/http_test.go
```

**Rate limit middleware spec:**
```go
func RateLimit(limiter Limiter, opts ...Option) func(http.Handler) http.Handler

// Options:
type options struct {
    KeyFunc      func(r *http.Request) string  // extract key from request
    OnLimited    func(w http.ResponseWriter, r *http.Request, result Result)
    SkipFunc     func(r *http.Request) bool    // skip rate limiting for this request
    ErrorHandler func(w http.ResponseWriter, r *http.Request, err error)
}

// Built-in KeyFuncs:
func KeyByIP() KeyFunc             // X-Forwarded-For → X-Real-IP → RemoteAddr
func KeyByHeader(name string) KeyFunc
func KeyByParam(name string) KeyFunc
func KeyFunc(fn func(*http.Request) string) KeyFunc
```

**Required response headers (RFC 6585 + IETF draft-ietf-httpapi-ratelimit-headers):**
```
X-RateLimit-Limit: <limit>
X-RateLimit-Remaining: <remaining>
X-RateLimit-Reset: <unix_timestamp_when_reset>
Retry-After: <seconds>           (only on 429 responses)
X-RateLimit-Policy: <policy_string>  e.g. "100;w=60" (100 per 60s)
```

**Default `OnLimited` handler:**
```go
// Returns HTTP 429 Too Many Requests with JSON body:
// {"error": "rate_limit_exceeded", "retry_after": 1.5, "limit": 100}
```

---

### T-304 [P1] — gRPC Interceptors

**Files to create:**
```
ratelimit/middleware/grpc.go
ratelimit/middleware/grpc_test.go
circuitbreaker/middleware/grpc.go
```

**Rate limit gRPC interceptor:**
```go
func UnaryServerInterceptor(limiter Limiter, opts ...Option) grpc.UnaryServerInterceptor
func StreamServerInterceptor(limiter Limiter, opts ...Option) grpc.StreamServerInterceptor

// On rate limit: return status.Error(codes.ResourceExhausted, "rate limit exceeded")
// Attach retry info to response metadata:
// grpc metadata key: "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"
```

---

## Phase 4 — Demo Server
> Goal: HTTP + WebSocket server that exposes the library for the frontend to consume.
> Verification gate: `curl http://localhost:8080/health/ready` returns 200. All WS endpoints stream data.

---

### T-401 [P0] — Server scaffolding and router

**Files to create:**
```
server/main.go
server/config/config.go
server/api/router.go
server/api/middleware.go
server/api/health.go
server/version/version.go
```

**Server startup order:**
1. Load and validate config (fail fast with all errors listed)
2. Initialize structured logger (`log/slog`, JSON in prod, text in dev)
3. Initialize all demo limiters (one instance per algorithm with demo config)
4. Initialize all demo circuit breakers (2 named CBs)
5. Start HTTP server with graceful shutdown (15s drain on SIGTERM)
6. Mark health/ready = true ONLY after all components initialized

**Middleware stack (in order):**
1. Request ID injection (`X-Request-ID` header or generate UUID)
2. Structured request logger (slog) with mandatory fields
3. CORS (configurable origins, default: localhost:3000)
4. Panic recovery → 500 with structured error response
5. Content-Type validation on POST routes

---

### T-402 [P0] — Rate limiter API handlers

**Files to create:**
```
server/api/ratelimit_handlers.go
server/api/ratelimit_handlers_test.go
```

**All endpoints must:**
- Accept `key` query param (default: `"demo"`)
- Return `application/json` with full `Result` struct
- Set all X-RateLimit-* headers
- Return 200 on Allow=true, 429 on Allow=false

**POST /api/v1/limiters/{algorithm}/allow:**
```json
Request body (optional): {"key": "user:123", "n": 1}
Response 200: {"allowed": true, "limit": 10, "remaining": 9, ...full Result...}
Response 429: {"allowed": false, "retry_after_ms": 150, ...full Result...}
```

**POST /api/v1/limiters/{algorithm}/configure:**
```json
Request body: {"limit": 20, "window_ms": 1000, "burst": 5}
Response 200: {"algorithm": "token_bucket", "config": {...new config...}}
```

---

### T-403 [P0] — Circuit breaker API handlers

**Files to create:**
```
server/api/circuitbreaker_handlers.go
server/api/circuitbreaker_handlers_test.go
```

**POST /api/v1/cb/{name}/execute:**
```json
Request: {"simulate": "success" | "failure" | "timeout" | "panic"}
Response 200: {"executed": true, "state": "closed", "metrics": {...Snapshot...}}
Response 503: {"executed": false, "state": "open", "error": "circuit_open", "retry_after_ms": 5000}
```

**POST /api/v1/cb/{name}/force-open, /force-close, /force-half-open:**
- Force state transitions for demo purposes
- Respond with new snapshot

---

### T-404 [P0] — WebSocket hub and real-time streaming

**Files to create:**
```
server/api/websocket.go
server/api/hub.go
```

**Hub pattern:**
```go
type Hub struct {
    clients    map[*Client]bool
    broadcast  chan []byte
    register   chan *Client
    unregister chan *Client
    mu         sync.RWMutex
}
```

**Streams (each runs a goroutine that publishes to Hub at interval):**
```
/ws/v1/limiters/{algorithm}     → State snapshot every 100ms
/ws/v1/cb/{name}                → CB Snapshot every 100ms
/ws/v1/cb/all                   → all CB snapshots every 200ms
/ws/v1/events                   → all allow/deny/CB events as they happen (pushed by handlers)
/ws/v1/simulation               → simulation progress (pushed by simulate handlers)
```

**Client cleanup:** on disconnect, unregister from hub immediately, goroutine exits via done channel.

---

### T-405 [P0] — Load simulation engine

**Files to create:**
```
server/simulation/engine.go
server/simulation/scenarios.go
server/api/simulate_handlers.go
```

**Simulation engine:**
```go
type Engine struct {
    active    atomic.Bool
    cancel    context.CancelFunc
    results   chan SimResult    // buffered 10000
    stats     *SimStats         // atomic stats updated every request
    mu        sync.Mutex
}

type SimResult struct {
    Timestamp  time.Time
    Allowed    bool
    Latency    time.Duration
    Algorithm  string
    Error      error
    CBState    string
}
```

**Built-in scenarios:**
```go
var Scenarios = map[string]ScenarioFn{
    "burst":           BurstScenario,        // spike load
    "gradual_ramp":    GradualRampScenario,  // 0 → max over duration
    "failure_inject":  FailureInjectScenario, // errors at configurable rate
    "thundering_herd": ThunderingHerdScenario, // all at once
    "steady_state":    SteadyStateScenario,   // constant rate
}
```

Simulation results streamed via `/ws/v1/simulation` WebSocket channel.

---

### T-406 [P0] — Prometheus metrics exporter

**Files to create:**
```
server/metrics/prometheus.go
server/metrics/collectors.go
```

**All metrics from the spec must be registered.** Use `prometheus/client_golang` (only in server package — not core library).

---

## Phase 5 — Next.js Frontend
> Goal: All pages implemented with live data from backend.
> Verification gate: `npm run check` passes. All pages load in < 2s. No TypeScript errors.

---

### T-501 [P0] — Project setup and design system

**Files to create:**
```
frontend/package.json          # all deps pinned with exact versions
frontend/tsconfig.json         # strict: true, no any
frontend/tailwind.config.ts    # design tokens as CSS vars
frontend/app/globals.css       # CSS custom properties
frontend/components/ui/        # shadcn/ui base components
frontend/lib/api/client.ts     # typed API client
frontend/lib/api/types.ts      # TypeScript interfaces matching Go types exactly
frontend/lib/ws/manager.ts     # WebSocket manager with reconnect
frontend/lib/stores/           # Zustand stores
frontend/components/layouts/   # AppShell, Sidebar
```

**`lib/api/types.ts` must define:**
- `Result`, `State`, `Metadata` matching Go types exactly
- `CBSnapshot`, `CBState` enum
- `Event` discriminated union for WebSocket events
- `SimResult`, `SimStats`
- All API response wrappers
- Zod schemas for runtime validation of all API responses

**`lib/ws/manager.ts` spec:**
```typescript
class WSManager {
    private connections = new Map<string, ManagedWS>()
    
    subscribe<T>(
        endpoint: string,
        schema: z.ZodType<T>,
        handler: (data: T) => void,
        options?: { reconnectDelay?: number; maxReconnectDelay?: number }
    ): () => void   // returns unsubscribe function
    
    // Auto-reconnects with exponential backoff: 500ms, 1s, 2s, 4s, max 30s
    // Validates all incoming messages against schema — invalid messages logged, not thrown
    // Connection state exposed as readable store: 'connecting' | 'connected' | 'disconnected'
}

export const wsManager = new WSManager()  // singleton
```

---

### T-502 [P0] — App layout, sidebar navigation, theme

**Files to create:**
```
frontend/app/layout.tsx
frontend/components/layouts/AppShell.tsx
frontend/components/layouts/Sidebar.tsx
frontend/components/layouts/TopBar.tsx
frontend/components/ui/StatusBadge.tsx     # reusable CB state + allow/deny badges
frontend/components/ui/MetricCard.tsx      # stat card with optional sparkline
frontend/components/ui/SkeletonCard.tsx    # skeleton loader matching MetricCard shape
frontend/components/ui/ErrorState.tsx      # inline error with retry
frontend/components/ui/EmptyState.tsx      # empty state with CTA
frontend/hooks/useWebSocket.ts             # React hook wrapping WSManager
frontend/hooks/usePoll.ts                  # polling hook with SWR
```

**Sidebar routes:**
```
/                          Overview
/algorithms/token-bucket   Token Bucket
/algorithms/leaky-bucket   Leaky Bucket
/algorithms/sliding-window Sliding Window
/algorithms/fixed-window   Fixed Window
/algorithms/gcra           GCRA
/algorithms/adaptive       Adaptive
/algorithms/compare        Compare
---
/circuit-breaker           Circuit Breaker
---
/pipeline                  Pipeline Builder
/simulate                  Simulator
---
/docs                      Documentation
```

---

### T-503 [P0] — Overview page

**File:** `frontend/app/page.tsx`

Displays: all algorithm state cards (polling /api/v1/limiters/{algo}/state every 500ms) + CB status badges + global stats. All cards have skeleton loaders on first load.

---

### T-504 [P0] — Algorithm deep dive pages (all 6)

**Files:**
```
frontend/app/algorithms/[algo]/page.tsx
frontend/components/algorithms/TokenBucketViz.tsx
frontend/components/algorithms/LeakyBucketViz.tsx
frontend/components/algorithms/SlidingWindowLogViz.tsx
frontend/components/algorithms/SlidingWindowCounterViz.tsx
frontend/components/algorithms/FixedWindowViz.tsx
frontend/components/algorithms/GCRAViz.tsx
frontend/components/algorithms/AdaptiveViz.tsx
frontend/components/algorithms/EventStream.tsx       # shared right panel
frontend/components/algorithms/AlgoControls.tsx      # shared left panel
```

**Each visualization must:**
- Be implemented with Framer Motion animations
- Show actual values from live API state (not hardcoded)
- Update when WebSocket events arrive
- Have `prefers-reduced-motion` media query — instant state change if user prefers no motion
- Be keyboard accessible (all interactive elements focusable)

**Token bucket animation spec (most important one):**
```
Bucket container: rounded rect SVG, height proportional to capacity
Water level: animated rect filling from bottom, height = (tokens/capacity) * 100%
  → animate height continuously using Framer Motion spring
Drip animation: circle drops falling from top at refillRate/s
  → one circle per token refilled, falls from rim to water level
Drain animation: when request allowed, circle drains from water level
Request denied: shake animation on bucket
Numbers overlay: current tokens (large), capacity, refill rate
```

---

### T-505 [P0] — Circuit breaker state machine page

**Files:**
```
frontend/app/circuit-breaker/page.tsx
frontend/components/cb/StateMachineViz.tsx    # SVG state diagram
frontend/components/cb/CBControls.tsx
frontend/components/cb/CBMetrics.tsx
frontend/components/cb/CBEventLog.tsx
frontend/components/cb/CBCharts.tsx
```

**StateMachineViz spec:**
- Three circles (CLOSED green, HALF-OPEN amber, OPEN red) in a triangle layout
- Animated arrows between states with transition labels
- Active state has `box-shadow: 0 0 20px <state-color>` pulsing animation
- When state changes (via WS event), animate the arrow in the direction of transition
- Hover arrow → tooltip showing transition condition

---

### T-506 [P0] — Load simulator page

**Files:**
```
frontend/app/simulate/page.tsx
frontend/components/simulation/ScenarioSelector.tsx
frontend/components/simulation/SimConfig.tsx
frontend/components/simulation/SimDashboard.tsx
frontend/components/simulation/RequestTimeline.tsx   # scatter plot
frontend/components/simulation/SimStats.tsx
```

---

### T-507 [P1] — Algorithm comparison page

**File:** `frontend/app/algorithms/compare/page.tsx`

Select 2-4 algorithms. Same config. Same requests. Side-by-side results with diff highlighting.

---

### T-508 [P1] — Pipeline builder page

**Files:**
```
frontend/app/pipeline/page.tsx
frontend/components/pipeline/PipelineBuilder.tsx
frontend/components/pipeline/StageCard.tsx
frontend/components/pipeline/FunnelChart.tsx
```

Drag-and-drop reordering using `@dnd-kit/core`. Toggle stages on/off. Config panel per stage.

---

### T-509 [P1] — Documentation pages

**Files:**
```
frontend/app/docs/page.tsx
frontend/app/docs/[slug]/page.tsx
frontend/content/docs/token-bucket.mdx
frontend/content/docs/leaky-bucket.mdx
frontend/content/docs/sliding-window.mdx
frontend/content/docs/gcra.mdx
frontend/content/docs/circuit-breaker.mdx
frontend/content/docs/comparison.mdx
```

Use `next-mdx-remote` with KaTeX for math rendering. `shiki` for syntax highlighting.

---

## Phase 6 — Observability, Production Hardening, Security
> Goal: OpenTelemetry, structured logging, security headers, memory safety verification.
> Verification gate: Prometheus metrics populated, all log lines have mandatory fields.

---

### T-601 [P0] — Structured logging with slog

**Files to create:**
```
server/logger/logger.go
server/logger/middleware.go
server/logger/logger_test.go
```

**Mandatory log fields on every request log line:**
```json
{
  "timestamp": "2025-01-01T00:00:00.000Z",
  "level": "INFO",
  "request_id": "uuid-v4",
  "method": "POST",
  "path": "/api/v1/limiters/token-bucket/allow",
  "status": 200,
  "duration_ms": 0.45,
  "algorithm": "token_bucket",
  "key": "demo",
  "allowed": true,
  "remaining": 9,
  "client_ip": "127.0.0.1",
  "user_agent": "..."
}
```

Use `log/slog` with JSON handler. `logger.FromCtx(ctx)` returns request-scoped logger with `request_id` pre-attached.

---

### T-602 [P1] — OpenTelemetry tracing

**Files to create:**
```
server/telemetry/tracer.go
server/telemetry/spans.go
```

OTel spans for: HTTP request (root), limiter.Allow() call, CB.Execute() call. OTLP exporter. No-op when `OTEL_ENABLED=false`.

---

### T-603 [P0] — Security headers and input validation

**Files to modify:**
```
server/api/middleware.go        # add security headers middleware
server/api/validation.go        # NEW: request body validators
```

**Security headers middleware (add to every response):**
```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Content-Security-Policy: default-src 'self'
Referrer-Policy: strict-origin-when-cross-origin
```

**Input validation:**
- All `key` parameters: validate non-empty, max 512 bytes, no null bytes, no newlines (header injection prevention)
- All numeric parameters: validate positive, within configured bounds
- Reject with 400 + structured error body on any validation failure

---

### T-604 [P0] — Goroutine leak detection in tests

**Files to create:**
```
internal/testutil/goroutine.go    # goroutine leak checker
internal/testutil/goroutine_test.go
```

```go
// LeakChecker records goroutine count before a test and verifies no net increase after.
type LeakChecker struct { before int }

func NewLeakChecker() *LeakChecker
func (c *LeakChecker) Check(t *testing.T)
// Allows up to 2 goroutines difference (Go scheduler goroutines can fluctuate)
```

**Add to ALL test files for algorithms with goroutines** (leaky bucket, adaptive limiter, cleanup goroutines):
```go
func TestLeakyBucket_CloseStopsLeaker(t *testing.T) {
    lc := testutil.NewLeakChecker()
    defer lc.Check(t)
    
    lb := leakybucket.New(10, 100)
    // ... test body ...
    lb.Close()
    // lc.Check verifies goroutine count returned to baseline
}
```

---

### T-605 [P0] — Memory safety: verify invariants under chaos

**Files to create:**
```
internal/testutil/chaos_test.go   # chaos testing helpers
```

Run each algorithm under concurrent chaos conditions:
- 1000 goroutines per key, 10 keys, 100k operations each
- All operations: Allow, AllowN, Peek, Reset, Wait with context cancel
- After run: verify 0 <= Remaining <= Limit for every Peek result
- Verify zero panics (recover in each goroutine)
- Run with `GOMAXPROCS=1` and `GOMAXPROCS=runtime.NumCPU()` separately

---

## Phase 7 — Infrastructure, CI/CD, Documentation
> Goal: Docker, Kubernetes, GitHub Actions, complete README, godoc polish.
> Verification gate: `docker-compose up` works. CI passes on clean branch. godoc looks professional.

---

### T-701 [P0] — Dockerfile and docker-compose

Create multi-stage Dockerfile (distroless runtime) and docker-compose with Redis + Prometheus + Grafana as specified in the infrastructure section of the prompt.

---

### T-702 [P0] — GitHub Actions CI pipeline

Create `.github/workflows/ci.yml` and `.github/workflows/release.yml` exactly as specified. Must include the `verify-zero-deps` job.

---

### T-703 [P0] — Kubernetes manifests

Create `deploy/kubernetes/` with Deployment, Service, ConfigMap, HPA, PDB as specified.

---

### T-704 [P0] — Complete README.md

**README must contain (in order):**
1. One-line description + badges (Go version, CI status, coverage, Go Report Card, pkg.go.dev)
2. Feature list (all algorithms with one-liner descriptions)
3. Quick start — 5-line Go code snippet to rate-limit HTTP requests
4. Algorithm comparison table:
   ```
   | Algorithm       | Burst | Exact | Memory  | Distributed | Use When              |
   |-----------------|-------|-------|---------|-------------|----------------------|
   | Token Bucket    | ✅    | ✅    | O(keys) | ✅          | API rate limiting    |
   | GCRA            | ✅    | ✅    | O(keys) | ✅          | High-performance API |
   | Sliding Log     | ❌    | ✅    | O(req)  | ✅          | Exact counting needed|
   | Fixed Window    | ❌    | ✅    | O(keys) | ✅          | Simple, fast         |
   ```
5. Performance benchmarks table (from actual `make bench` output)
6. Full API documentation (all public types/functions with examples)
7. Distributed usage with Redis
8. HTTP middleware usage
9. Circuit breaker usage
10. Pipeline usage
11. How to run the demo server + frontend
12. Contributing guide
13. License

---

### T-705 [P0] — godoc polish pass

For every package, verify:
- Package-level godoc comment explains the algorithm with a reference link
- Every exported type has a doc comment ending in a period
- Every exported function has a doc comment starting with the function name
- `Example_*` functions exist for: basic usage, distributed usage, middleware usage
- Run `go vet ./...` — zero warnings
- Run `golangci-lint run` — zero issues

---

### T-706 [P1] — Examples directory

**Files to create:**
```
examples/http-server/main.go       # complete HTTP server with rate limiting
examples/grpc-server/main.go       # gRPC server with interceptors
examples/distributed/main.go       # Redis-backed distributed limiting
examples/pipeline/main.go          # full resilience pipeline
```

Each example must be a standalone runnable Go program with comments explaining every line.

---

## Phase 8 — Final Verification & Release
> Run everything. Fix everything. Tag v1.0.0.

---

### T-801 [P0] — Full test suite verification

```bash
make test-race           # must pass with zero races
make fuzz-long           # 5 min per fuzz target, zero panics
make bench               # all targets met
make verify-deps         # zero external deps in core
golangci-lint run        # zero linter issues
cd frontend && npm run check && npm run build  # zero TS errors, builds
```

---

### T-802 [P0] — End-to-end smoke test

```bash
docker-compose up -d
sleep 10

# DNS resolver is rate-limited
curl -s http://localhost:8080/health/ready   # must return 200

# Rate limiter: send 15 requests to 10 req/s limit
for i in $(seq 1 15); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    http://localhost:8080/api/v1/limiters/token-bucket/allow)
  echo "Request $i: $STATUS"
done
# Must see 10x 200 then 5x 429

# Circuit breaker: force open
curl -X POST http://localhost:8080/api/v1/cb/demo/force-open
curl -s http://localhost:8080/api/v1/cb/demo/state | jq .state  # must be "open"

# Prometheus metrics
curl -s http://localhost:8080/metrics | grep resilience_ratelimit_requests_total

docker-compose down
```

---

### T-803 [P0] — Release checklist

- [ ] All T-801 commands pass
- [ ] T-802 smoke test passes
- [ ] README badges all green
- [ ] CHANGELOG updated for v1.0.0
- [ ] All godoc Examples render correctly at `godoc -http=:6060`
- [ ] `git tag v1.0.0 && git push --tags` — triggers release workflow
- [ ] pkg.go.dev shows new version within 5 minutes

---

## Summary Table

| Phase | Tickets | Description | Gate |
|-------|---------|-------------|------|
| 0 | T-001 to T-005 | Foundation, scaffolding, shared internals | `go build ./...` passes |
| 1 | T-101 to T-108 | All 7 rate limiting algorithms | `make test-race` + benchmarks pass |
| 2 | T-201 to T-206 | Circuit breaker + resilience patterns | `make test-race` passes |
| 3 | T-301 to T-304 | Redis distributed + HTTP/gRPC middleware | Integration tests pass |
| 4 | T-401 to T-406 | Demo HTTP/WebSocket server | Health ready, WS streams data |
| 5 | T-501 to T-509 | Next.js frontend, all pages | `npm run check` + all pages load |
| 6 | T-601 to T-605 | Observability, security, memory safety | Prometheus populated, no goroutine leaks |
| 7 | T-701 to T-706 | Docker, K8s, CI/CD, docs | `docker-compose up` works, CI passes |
| 8 | T-801 to T-803 | Final verification + release | All checks pass, v1.0.0 tagged |

**Total: 36 tickets across 8 phases**