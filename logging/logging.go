// Package logging provides a zero-dependency structured-logging bridge that
// turns a stdlib *slog.Logger into the hook types the resilience core already
// understands. It lets library consumers observe rate-limit decisions,
// circuit-breaker state changes, and breaker execution outcomes as structured
// logs without pulling in any external dependency: slog is part of the Go
// standard library, so the core zero-dependency guarantee is preserved.
//
// Two entry points cover the two hook shapes the core exposes:
//
//   - [NewRecorder] returns a [metric.Recorder] that emits a structured log line
//     for every event a limiter, circuit breaker, or bulkhead reports through
//     the recorder. Wire it with the existing WithRecorder option, e.g.
//     tokenbucket.WithRecorder(logging.NewRecorder(slog.Default())).
//
//   - [CircuitBreakerHooks] returns a set of callback functions matching the
//     fields of circuitbreaker.Config (OnStateChange / OnSuccess / OnFailure /
//     OnRejected), so a consumer can log breaker lifecycle events directly.
//
// All logging is level-gated (via slog's own Enabled checks) so a disabled
// level costs only a cheap comparison on the hot path. A nil *slog.Logger is
// treated as slog.Default(). Every constructor accepts functional options to
// tune the emitted levels and a static log-group name.
package logging

import (
	"context"
	"log/slog"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// Default levels used when no override option is supplied. Decisions and
// per-request outcomes are high-volume, so they default to DEBUG; lifecycle
// events (state transitions) default to INFO.
const (
	defaultDecisionLevel   = slog.LevelDebug
	defaultOutcomeLevel    = slog.LevelDebug
	defaultTransitionLevel = slog.LevelInfo
)

// config holds the tunable knobs shared by NewRecorder and CircuitBreakerHooks.
type config struct {
	logger          *slog.Logger
	group           string
	decisionLevel   slog.Level
	outcomeLevel    slog.Level
	transitionLevel slog.Level
}

func newConfig(logger *slog.Logger, opts ...Option) config {
	if logger == nil {
		logger = slog.Default()
	}
	c := config{
		logger:          logger,
		decisionLevel:   defaultDecisionLevel,
		outcomeLevel:    defaultOutcomeLevel,
		transitionLevel: defaultTransitionLevel,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	if c.group != "" {
		c.logger = c.logger.WithGroup(c.group)
	}
	return c
}

// Option configures a logging bridge (recorder or hook set).
type Option func(*config)

// WithGroup nests every attribute the bridge emits under the given slog group
// name. An empty string (the default) adds no group.
func WithGroup(name string) Option {
	return func(c *config) { c.group = name }
}

// WithDecisionLevel overrides the level used for rate-limit decision logs
// (default slog.LevelDebug).
func WithDecisionLevel(l slog.Level) Option {
	return func(c *config) { c.decisionLevel = l }
}

// WithOutcomeLevel overrides the level used for per-request circuit-breaker
// outcome logs — success, failure, and rejection (default slog.LevelDebug).
func WithOutcomeLevel(l slog.Level) Option {
	return func(c *config) { c.outcomeLevel = l }
}

// WithTransitionLevel overrides the level used for circuit-breaker state and
// transition logs (default slog.LevelInfo).
func WithTransitionLevel(l slog.Level) Option {
	return func(c *config) { c.transitionLevel = l }
}

// recorder is a metric.Recorder that emits structured logs. It is safe for
// concurrent use because *slog.Logger is.
type recorder struct {
	log             *slog.Logger
	decisionLevel   slog.Level
	outcomeLevel    slog.Level
	transitionLevel slog.Level
}

// NewRecorder returns a metric.Recorder that logs every recorded event as a
// structured slog record. Pass it to any limiter/breaker/bulkhead via their
// WithRecorder option. A nil logger uses slog.Default(). The returned recorder
// is safe for concurrent use and level-gated, so disabled levels are cheap.
func NewRecorder(logger *slog.Logger, opts ...Option) metric.Recorder {
	c := newConfig(logger, opts...)
	return &recorder{
		log:             c.logger,
		decisionLevel:   c.decisionLevel,
		outcomeLevel:    c.outcomeLevel,
		transitionLevel: c.transitionLevel,
	}
}

// log emits a record at level lvl if enabled. Attributes are only built when
// the level is enabled, keeping disabled paths allocation-free.
func (r *recorder) logAt(lvl slog.Level, msg string, attrs ...slog.Attr) {
	ctx := context.Background()
	if !r.log.Enabled(ctx, lvl) {
		return
	}
	r.log.LogAttrs(ctx, lvl, msg, attrs...)
}

func (r *recorder) IncAllowed(algorithm string) {
	r.logAt(r.decisionLevel, "ratelimit decision",
		slog.String("algorithm", algorithm),
		slog.Bool("allowed", true),
	)
}

func (r *recorder) IncDenied(algorithm string) {
	r.logAt(r.decisionLevel, "ratelimit decision",
		slog.String("algorithm", algorithm),
		slog.Bool("allowed", false),
	)
}

func (r *recorder) ObserveDecision(algorithm string, d time.Duration) {
	r.logAt(r.decisionLevel, "ratelimit decision latency",
		slog.String("algorithm", algorithm),
		slog.Duration("duration", d),
	)
}

func (r *recorder) RecordCBState(name, state string) {
	r.logAt(r.transitionLevel, "circuitbreaker state",
		slog.String("name", name),
		slog.String("state", state),
	)
}

func (r *recorder) IncCBResult(name, result string) {
	r.logAt(r.outcomeLevel, "circuitbreaker result",
		slog.String("name", name),
		slog.String("result", result),
	)
}

func (r *recorder) ObserveCBExecution(name string, d time.Duration) {
	r.logAt(r.outcomeLevel, "circuitbreaker execution",
		slog.String("name", name),
		slog.Duration("duration", d),
	)
}

func (r *recorder) IncCBTransition(name, from, to string) {
	r.logAt(r.transitionLevel, "circuitbreaker transition",
		slog.String("name", name),
		slog.String("from", from),
		slog.String("to", to),
	)
}

func (r *recorder) SetBulkheadInflight(name string, n int) {
	r.logAt(r.decisionLevel, "bulkhead inflight",
		slog.String("name", name),
		slog.Int("inflight", n),
	)
}

func (r *recorder) IncBulkheadRejected(name string) {
	r.logAt(r.outcomeLevel, "bulkhead rejected",
		slog.String("name", name),
	)
}

// CircuitBreakerHooks is a set of callback functions whose fields are directly
// assignable to the matching fields of circuitbreaker.Config. Each hook logs a
// structured record; unset (nil) values are never returned, so the whole set
// can be spread into a Config, e.g.:
//
//	h := logging.CircuitBreakerHooks(slog.Default())
//	cfg := circuitbreaker.Config{
//		Name:          "payments",
//		OnStateChange: h.OnStateChange,
//		OnSuccess:     h.OnSuccess,
//		OnFailure:     h.OnFailure,
//		OnRejected:    h.OnRejected,
//	}
//
// The State parameter of OnStateChange is typed as fmt.Stringer so this package
// need not import circuitbreaker (avoiding an import cycle) while still logging
// the human-readable state name. circuitbreaker.State satisfies fmt.Stringer.
type CircuitBreakerHooks struct {
	// OnStateChange logs a transition between two states.
	OnStateChange func(name string, from, to Stringer)
	// OnSuccess logs a successful call and its duration.
	OnSuccess func(name string, duration time.Duration)
	// OnFailure logs a failed call, its duration, and the error.
	OnFailure func(name string, duration time.Duration, err error)
	// OnRejected logs a call rejected because the breaker was open / at the
	// half-open probe limit.
	OnRejected func(name string)
}

// Stringer mirrors fmt.Stringer. It is the parameter type used for
// circuitbreaker states in CircuitBreakerHooks.OnStateChange so that
// circuitbreaker.State (which has a String method) can be passed without this
// package importing circuitbreaker.
type Stringer interface{ String() string }

// NewCircuitBreakerHooks returns a CircuitBreakerHooks whose callbacks emit
// structured slog records. A nil logger uses slog.Default(). All hooks are
// level-gated.
func NewCircuitBreakerHooks(logger *slog.Logger, opts ...Option) CircuitBreakerHooks {
	c := newConfig(logger, opts...)
	log := c.logger
	ctx := context.Background()

	return CircuitBreakerHooks{
		OnStateChange: func(name string, from, to Stringer) {
			if !log.Enabled(ctx, c.transitionLevel) {
				return
			}
			log.LogAttrs(ctx, c.transitionLevel, "circuitbreaker transition",
				slog.String("name", name),
				slog.String("from", stringify(from)),
				slog.String("to", stringify(to)),
			)
		},
		OnSuccess: func(name string, duration time.Duration) {
			if !log.Enabled(ctx, c.outcomeLevel) {
				return
			}
			log.LogAttrs(ctx, c.outcomeLevel, "circuitbreaker success",
				slog.String("name", name),
				slog.Duration("duration", duration),
			)
		},
		OnFailure: func(name string, duration time.Duration, err error) {
			if !log.Enabled(ctx, c.outcomeLevel) {
				return
			}
			log.LogAttrs(ctx, c.outcomeLevel, "circuitbreaker failure",
				slog.String("name", name),
				slog.Duration("duration", duration),
				slog.Any("error", err),
			)
		},
		OnRejected: func(name string) {
			if !log.Enabled(ctx, c.outcomeLevel) {
				return
			}
			log.LogAttrs(ctx, c.outcomeLevel, "circuitbreaker rejected",
				slog.String("name", name),
			)
		},
	}
}

// stringify returns s.String() or "<nil>" for a nil interface value.
func stringify(s Stringer) string {
	if s == nil {
		return "<nil>"
	}
	return s.String()
}
