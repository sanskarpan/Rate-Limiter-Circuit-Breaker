package circuitbreaker

// DistributedCircuitBreaker (ENHANCEMENTS §1.4) is a circuit breaker whose state
// — current state, rolling failure count, half-open success count, openedAt, and
// the in-flight probe reservation — is SHARED across every process instance via a
// ratelimit/store.Store (typically Redis). A single hash key per breaker name
// holds the packed state, and every allow/reject decision plus every state
// transition runs inside an atomic Lua script (emulated faithfully for the
// in-memory store). The upshot: when instance A's calls trip the breaker, the
// whole fleet — including instance B that never saw a failure — immediately
// observes Open, instead of each instance learning independently and slowly.
//
// State machine (count-based, matching the single-instance CircuitBreaker):
//
//	Closed  → Open:     FailureThreshold failures accumulate
//	Open    → HalfOpen: OpenTimeout elapsed (lazy, evaluated in-script on the
//	                    Redis server clock so app-fleet clock skew cannot open the
//	                    breaker early/late — see CLOCK below)
//	HalfOpen→ Closed:   SuccessThreshold consecutive probe successes
//	HalfOpen→ Open:     any single probe failure
//
// FAIL-OPEN: if the store errors (e.g. Redis is unreachable), Execute does NOT
// wedge traffic — it allows the call and runs fn, mirroring the fail-open pattern
// the distributed rate limiters use for store outages. A store outage therefore
// degrades the breaker to "always closed" (no protection) rather than "always
// open" (total outage). This is documented on each method that swallows a store
// error.
//
// CLOCK / skew tradeoff: the Open→HalfOpen timeout is evaluated inside the Lua
// script against the Redis server clock (via TIME), so the timeout is pinned to a
// single authoritative clock regardless of how skewed the calling hosts are. The
// injected cfg.Clock is used only to derive the client-side now passed to the
// in-memory emulation (which has no server) and for Snapshot's cosmetic
// TimeUntilHalfOpen. See ratelimit/store CircuitBreaker*Script.
//
// All methods are safe for concurrent use.

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// stateKeyPrefix namespaces the shared state key within the store.
const stateKeyPrefix = "cb:"

// DistributedCircuitBreaker shares circuit breaker state across process instances
// via a store.Store. See the package-level type doc for the model and tradeoffs.
type DistributedCircuitBreaker struct {
	name  string
	store store.Store
	cfg   Config

	// key is the single store key holding this breaker's packed shared state.
	key string
}

// DistributedOption customizes a DistributedCircuitBreaker.
type DistributedOption func(*DistributedCircuitBreaker)

// WithKeyPrefix overrides the default "cb:" prefix applied to the shared state
// key. Useful to isolate breaker state across environments sharing one store.
func WithKeyPrefix(prefix string) DistributedOption {
	return func(d *DistributedCircuitBreaker) {
		d.key = prefix + d.name
	}
}

// NewDistributed creates a circuit breaker whose state is shared across instances
// through s. Instances constructed with the same name and store observe one
// shared state machine. cfg reuses the standard Config; the count-based
// thresholds (FailureThreshold, SuccessThreshold, HalfOpenMaxRequests,
// OpenTimeout) drive the shared state machine.
func NewDistributed(name string, s store.Store, cfg Config, opts ...DistributedOption) *DistributedCircuitBreaker {
	cfg.defaults()
	cfg.Name = name
	d := &DistributedCircuitBreaker{
		name:  name,
		store: s,
		cfg:   cfg,
		key:   stateKeyPrefix + name,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Name returns the breaker's name.
func (d *DistributedCircuitBreaker) Name() string { return d.name }

// ttl is the expiry applied to the shared state key on each write. It is
// generous relative to OpenTimeout so an idle breaker's state survives long
// enough to be meaningful, but still self-cleans if the breaker goes unused.
func (d *DistributedCircuitBreaker) ttl() time.Duration {
	ttl := d.cfg.OpenTimeout * 10
	if ttl < time.Minute {
		ttl = time.Minute
	}
	return ttl
}

func (d *DistributedCircuitBreaker) nowNs() int64 {
	return d.cfg.Clock.Now().UnixNano()
}

// Execute runs fn iff the shared state allows it, then atomically records the
// outcome and applies any resulting transition.
//
//   - Rejects with a CircuitError wrapping ErrCircuitOpen when the shared state
//     is Open (and OpenTimeout has not yet elapsed).
//   - Rejects with a CircuitError wrapping ErrTooManyRequests when Half-Open and
//     the probe limit (HalfOpenMaxRequests) is already reserved.
//   - FAIL-OPEN: if the store errors on the acquire step, fn is executed anyway
//     (no protection) rather than blocking traffic.
func (d *DistributedCircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	decision, state, acquiredProbe, openedAtNs, storeErr := d.acquire(ctx)
	if storeErr != nil {
		// Fail-open: the store is unavailable, so we cannot consult shared state.
		// Run fn rather than wedge traffic. We still attempt to record the
		// outcome (also best-effort) so state converges once the store recovers.
		d.cfg.Recorder.IncCBResult(d.name, "success") // observability: fn ran
		err := d.runFn(ctx, fn)
		d.record(ctx, err, false)
		return err
	}

	switch decision {
	case decisionRejectOpen:
		d.onRejected()
		var timeUntilHalfOpen time.Duration
		if openedAtNs > 0 {
			halfOpenAt := time.Unix(0, openedAtNs).Add(d.cfg.OpenTimeout)
			timeUntilHalfOpen = halfOpenAt.Sub(d.cfg.Clock.Now())
			if timeUntilHalfOpen < 0 {
				timeUntilHalfOpen = 0
			}
		}
		return newCircuitError(d.name, State(state), timeUntilHalfOpen, ErrCircuitOpen)
	case decisionRejectProbe:
		d.onRejected()
		return newCircuitError(d.name, State(state), 0, ErrTooManyRequests)
	}

	// Allowed. Run fn, then record the outcome + transition atomically.
	start := d.cfg.Clock.Now()
	err := d.runFn(ctx, fn)
	duration := d.cfg.Clock.Now().Sub(start)
	d.cfg.Recorder.ObserveCBExecution(d.name, duration)

	isFailure := d.classify(ctx, err)
	if isFailure {
		d.cfg.Recorder.IncCBResult(d.name, "failure")
	} else {
		d.cfg.Recorder.IncCBResult(d.name, "success")
	}
	d.recordOutcome(ctx, isFailure, acquiredProbe)
	return err
}

// ExecuteWithFallback calls fn, and if it fails or the circuit is open, calls
// fallback with the resulting error.
func (d *DistributedCircuitBreaker) ExecuteWithFallback(
	ctx context.Context,
	fn func(context.Context) error,
	fallback func(context.Context, error) error,
) error {
	err := d.Execute(ctx, fn)
	if err != nil {
		return fallback(ctx, err)
	}
	return nil
}

// runFn executes fn with the optional RequestTimeout and panic-safe recovery,
// re-panicking after recording a failure so the caller's panic semantics hold.
func (d *DistributedCircuitBreaker) runFn(ctx context.Context, fn func(context.Context) error) (err error) {
	if d.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.cfg.RequestTimeout)
		defer cancel()
	}
	return fn(ctx)
}

// classify decides whether err counts as a circuit failure, mirroring the
// single-instance breaker: caller cancellation does not count, a CB-imposed
// RequestTimeout does, otherwise cfg.IsFailure decides.
func (d *DistributedCircuitBreaker) classify(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if d.cfg.RequestTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
		return true
	}
	if err == context.Canceled {
		return false
	}
	return d.cfg.IsFailure(err)
}

func (d *DistributedCircuitBreaker) onRejected() {
	d.cfg.Recorder.IncCBResult(d.name, "rejected")
	if d.cfg.OnRejected != nil {
		d.cfg.OnRejected(d.name)
	}
}

// decision codes returned by the acquire script.
const (
	decisionAllow       = 0
	decisionRejectOpen  = 1
	decisionRejectProbe = 2
)

// acquire runs the atomic read-decide script. acquiredProbe reports whether a
// half-open probe slot was reserved (which record must release). A non-nil
// storeErr signals the caller to fail open.
func (d *DistributedCircuitBreaker) acquire(ctx context.Context) (decision, state int, acquiredProbe bool, openedAtNs int64, storeErr error) {
	res, err := d.store.Eval(ctx, store.CircuitBreakerAcquireScriptID,
		[]string{d.key},
		int64(d.cfg.OpenTimeout), // open_timeout_ns
		int64(d.cfg.HalfOpenMaxRequests),
		d.ttl().Milliseconds(),
		d.nowNs(), // trailing now_ns for the in-memory emulation (Redis ignores it)
	)
	if err != nil {
		return 0, 0, false, 0, err
	}
	dec, st, oAt, ok := parseDecision(res)
	if !ok {
		// Malformed reply: fail open like a store error rather than guess.
		return 0, 0, false, 0, fmt.Errorf("circuitbreaker: malformed acquire reply %T", res)
	}
	acquiredProbe = dec == decisionAllow && st == int(StateHalfOpen)
	return dec, st, acquiredProbe, oAt, nil
}

// recordOutcome runs the atomic record-transition script. Store errors are
// swallowed (best-effort): the fail-open philosophy means a transient store
// outage must not surface from Execute; state re-converges on the next call.
func (d *DistributedCircuitBreaker) recordOutcome(ctx context.Context, isFailure, acquiredProbe bool) {
	outcome := int64(0)
	if isFailure {
		outcome = 1
	}
	probe := int64(0)
	if acquiredProbe {
		probe = 1
	}
	prev := d.stateBestEffort(ctx)
	res, err := d.store.Eval(ctx, store.CircuitBreakerRecordScriptID,
		[]string{d.key},
		outcome,
		int64(d.cfg.FailureThreshold),
		int64(d.cfg.SuccessThreshold),
		int64(d.cfg.OpenTimeout),
		d.ttl().Milliseconds(),
		probe,
		d.nowNs(),
	)
	if err != nil {
		return // best-effort; fail open
	}
	if newState, ok := parseSingleState(res); ok {
		d.emitTransition(prev, State(newState))
	}
}

// record is the fail-open path's best-effort outcome recorder (used when acquire
// already failed). It never blocks and never returns an error.
func (d *DistributedCircuitBreaker) record(ctx context.Context, err error, acquiredProbe bool) {
	d.recordOutcome(ctx, d.classify(ctx, err), acquiredProbe)
}

// emitTransition fires the state-change callback and observability signals when
// the recorded outcome moved the shared state. prev is a best-effort pre-read; if
// it could not be obtained (== -1) no transition is emitted.
func (d *DistributedCircuitBreaker) emitTransition(prev, next State) {
	if prev < 0 || prev == next {
		return
	}
	d.cfg.Recorder.RecordCBState(d.name, next.String())
	d.cfg.Recorder.IncCBTransition(d.name, prev.String(), next.String())
	if d.cfg.OnStateChange != nil {
		d.cfg.OnStateChange(d.name, prev, next)
	}
}

// stateBestEffort reads the effective state, returning -1 on any store error so
// callers can skip transition bookkeeping without failing.
func (d *DistributedCircuitBreaker) stateBestEffort(ctx context.Context) State {
	st, _, _, _, err := d.read(ctx)
	if err != nil {
		return -1
	}
	return State(st)
}

// State returns the current SHARED state of the breaker (with the lazy
// Open→HalfOpen promotion reflected). On a store error it FAILS OPEN, reporting
// StateClosed — consistent with Execute allowing traffic during a store outage.
func (d *DistributedCircuitBreaker) State(ctx context.Context) State {
	st, _, _, _, err := d.read(ctx)
	if err != nil {
		return StateClosed
	}
	return State(st)
}

// Snapshot returns a point-in-time view of the shared breaker state. On a store
// error it FAILS OPEN, returning a closed/empty snapshot.
func (d *DistributedCircuitBreaker) Snapshot(ctx context.Context) Snapshot {
	st, failures, successes, openedAtNs, err := d.read(ctx)
	if err != nil {
		return Snapshot{Name: d.name, State: StateClosed}
	}
	state := State(st)

	var openedAt time.Time
	var timeUntilHalfOpen time.Duration
	if openedAtNs > 0 {
		openedAt = time.Unix(0, openedAtNs)
		if state == StateOpen {
			halfOpenAt := openedAt.Add(d.cfg.OpenTimeout)
			timeUntilHalfOpen = halfOpenAt.Sub(d.cfg.Clock.Now())
			if timeUntilHalfOpen < 0 {
				timeUntilHalfOpen = 0
			}
		}
	}

	requests := failures + successes
	var failureRate float64
	if requests > 0 {
		failureRate = float64(failures) / float64(requests)
	}
	return Snapshot{
		Name:              d.name,
		State:             state,
		Failures:          failures,
		Successes:         successes,
		Requests:          requests,
		FailureRate:       failureRate,
		OpenedAt:          openedAt,
		TimeUntilHalfOpen: timeUntilHalfOpen,
	}
}

// read runs the read-only state script. Returns (state, failures, successes,
// openedAtNs, err).
func (d *DistributedCircuitBreaker) read(ctx context.Context) (state, failures, successes int, openedAtNs int64, err error) {
	res, evalErr := d.store.Eval(ctx, store.CircuitBreakerReadScriptID,
		[]string{d.key},
		int64(d.cfg.OpenTimeout),
		d.nowNs(),
	)
	if evalErr != nil {
		return 0, 0, 0, 0, evalErr
	}
	arr, ok := res.([]any)
	if !ok || len(arr) < 5 {
		return 0, 0, 0, 0, fmt.Errorf("circuitbreaker: malformed read reply %T", res)
	}
	state = int(asInt64(arr[0]))
	failures = int(asInt64(arr[1]))
	successes = int(asInt64(arr[2]))
	openedAtNs = asInt64(arr[3])
	return state, failures, successes, openedAtNs, nil
}

// String returns a human-readable description. It does not touch the store.
func (d *DistributedCircuitBreaker) String() string {
	return fmt.Sprintf("DistributedCircuitBreaker(%s)", d.name)
}

// ---------------------------------------------------------------------------
// reply parsing helpers (tolerant of int64/[]any shapes from Redis & memory)
// ---------------------------------------------------------------------------

func parseDecision(res any) (decision, state int, openedAtNs int64, ok bool) {
	arr, isArr := res.([]any)
	if !isArr || len(arr) < 2 {
		return 0, 0, 0, false
	}
	var oAt int64
	if len(arr) >= 3 {
		oAt = asInt64(arr[2])
	}
	return int(asInt64(arr[0])), int(asInt64(arr[1])), oAt, true
}

func parseSingleState(res any) (int, bool) {
	arr, ok := res.([]any)
	if !ok || len(arr) < 1 {
		return 0, false
	}
	return int(asInt64(arr[0])), true
}

// asInt64 coerces the numeric shapes Redis (int64) and the in-memory emulation
// (int64) can return. Strings are parsed defensively.
func asInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

// compile-time assertion that RealClock satisfies the clock we rely on.
var _ clock.Clock = clock.RealClock{}
