# Adaptive concurrency + load shedding under overload

Rate limits cap the *rate* of requests; they don't react to how the downstream
is actually coping. Two complementary tools do:

- **Adaptive concurrency** (`concurrency`) — instead of a fixed
  max-in-flight, a strategy (AIMD / Gradient2 / Vegas) continuously retunes the
  concurrency limit from observed latency. When latency climbs, the limit
  shrinks automatically.
- **Load shedding** (`loadshed`) — a CoDel-style controller that drops requests
  when the standing queue sojourn time stays high, honouring a per-request
  priority so critical traffic survives while best-effort traffic is shed first.

Both plug into the `pipeline` builder. In the canonical pipeline order the load
shedder runs first (admission control, before any resource is acquired) and the
concurrency limiter runs as the bulkhead stage.

## Adaptive concurrency limiter

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"

// Gradient2 adapts the limit toward the latency gradient; AIMD and Vegas are
// alternative strategies with the same constructor shape.
lim := concurrency.NewGradient2(concurrency.Config{
	InitialLimit: 20,
	MinLimit:     1,
	MaxLimit:     200,
	RTTTolerance: 2.0, // allow RTT up to 2x baseline before shrinking
})
```

Used directly, `Acquire` is non-blocking and `Wait` blocks for a slot. You must
call the returned `ReleaseFunc` exactly once, passing an `Outcome` so the
strategy learns from the result:

```go
release, ok := lim.Acquire(ctx)
if !ok {
	// over the current adaptive limit — shed or queue
	http.Error(w, "overloaded", http.StatusServiceUnavailable)
	return
}
start := time.Now()
err := callDownstream(ctx)
release(concurrency.Outcome{
	RTT:     time.Since(start),
	Dropped: err != nil, // timeouts/errors are the strongest overload signal
})
```

`lim.Limit()` and `lim.Inflight()` expose the current adaptive limit and live
in-flight count for monitoring.

## Load shedder

The shedder reads a priority from the request context. Attach one with
`loadshed.WithPriority`; the convenience tiers are `PriorityLow` (-1),
`PriorityDefault` (0), `PriorityHigh` (1), and `PriorityCritical` (2).

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"

shedder := loadshed.New(loadshed.Config{
	Target:   20 * time.Millisecond,   // acceptable standing-queue sojourn
	Interval: 100 * time.Millisecond,  // control window
})

// Tag critical requests so they survive shedding longest.
ctx = loadshed.WithPriority(ctx, loadshed.PriorityCritical)

accept, done := shedder.Admit(ctx)
if !accept {
	http.Error(w, "shedding load", http.StatusServiceUnavailable)
	return
}
defer done() // records the sojourn so the controller can adapt
// ... handle the request
```

`shedder.Dropping()` reports whether the controller is currently shedding, and
`shedder.LastSojourn()` the most recent queue delay — both handy for dashboards.

## Both, wired into a pipeline

```go
import (
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
)

conc := concurrency.NewGradient2(concurrency.Config{
	InitialLimit: 20, MinLimit: 1, MaxLimit: 200, RTTTolerance: 2.0,
})
shedder := loadshed.New(loadshed.Config{
	Target: 20 * time.Millisecond, Interval: 100 * time.Millisecond,
})

p := pipeline.New().
	LoadShed(shedder).   // admission control first — sheds before acquiring anything
	Concurrency(conc).   // adaptive bulkhead stage
	Timeout(2 * time.Second).
	Build()

// Priority flows through the context into the LoadShed stage.
ctx = loadshed.WithPriority(ctx, loadshed.PriorityHigh)
err := p.Execute(ctx, func(ctx context.Context) error {
	return handle(ctx)
})
// A shed request returns pipeline.ErrLoadShed.
if errors.Is(err, pipeline.ErrLoadShed) {
	// return 503 with Retry-After
}
```

The concurrency limiter measures each `Execute` and feeds the RTT back to its
strategy automatically when used as a pipeline stage, so the limit self-tunes
without any manual `Outcome` bookkeeping.

## See also

- [Protect a flaky downstream](flaky-downstream-cb-retry-hedge.md)
- [Observability: Prometheus + OTel](observability-prometheus-otel.md)
</content>
