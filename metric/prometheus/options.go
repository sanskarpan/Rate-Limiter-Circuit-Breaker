package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Option configures the Prometheus Recorder built by New. Options are additive
// and backward-compatible: with no options the Recorder behaves exactly as it
// did before options were introduced (classic fixed-bucket histograms, no
// exemplars).
type Option func(*config)

// config holds the resolved, internal configuration for a Recorder. It is not
// exported; callers mutate it only through Options.
type config struct {
	nativeHistograms bool
	nativeFactor     float64
	nativeMaxBuckets uint32
	nativeMinReset   time.Duration
	exemplars        bool
}

// defaultConfig returns the zero-behavior configuration: classic histograms,
// no exemplars. These defaults preserve the pre-options behavior exactly.
func defaultConfig() config {
	return config{
		nativeHistograms: false,
		nativeFactor:     defaultNativeBucketFactor,
		nativeMaxBuckets: defaultNativeMaxBuckets,
		nativeMinReset:   defaultNativeMinReset,
		exemplars:        false,
	}
}

// Native-histogram defaults, applied only when WithNativeHistograms is used
// without an explicit factor. A factor of 1.1 gives ~10% relative bucket
// resolution, matching the value recommended by the Prometheus native-histogram
// documentation as a sensible starting point.
const (
	defaultNativeBucketFactor = 1.1
	defaultNativeMaxBuckets   = 160
	defaultNativeMinReset     = time.Hour
)

// WithNativeHistograms enables Prometheus native (sparse, exponential)
// histograms on the duration histograms (rate-limit decision latency and
// circuit-breaker execution latency) IN ADDITION to the existing classic
// fixed buckets. Scrapers that understand native histograms ingest the
// high-resolution sparse representation; older scrapers continue to see the
// classic buckets unchanged, so this is safe to enable everywhere.
//
// factor is the NativeHistogramBucketFactor: an upper bound on the growth ratio
// between consecutive buckets (e.g. 1.1 ≈ 10% resolution). It must be > 1; any
// value ≤ 1 is replaced by the default of 1.1. Larger factors use fewer buckets
// (coarser); smaller factors use more (finer).
//
// The bucket count is bounded (NativeHistogramMaxBucketNumber) with a periodic
// reset (NativeHistogramMinResetDuration) so a pathological latency distribution
// cannot grow the in-memory bucket set without limit.
func WithNativeHistograms(factor float64) Option {
	return func(c *config) {
		c.nativeHistograms = true
		if factor > 1 {
			c.nativeFactor = factor
		} else {
			c.nativeFactor = defaultNativeBucketFactor
		}
	}
}

// WithNativeHistogramLimits tunes the safety bounds used when native histograms
// are enabled: maxBuckets caps the number of populated sparse buckets kept in
// memory per series (NativeHistogramMaxBucketNumber), and minReset is the
// minimum time between automatic bucket-schema resets
// (NativeHistogramMinResetDuration). Non-positive values leave the respective
// default in place. It is a no-op unless WithNativeHistograms is also supplied.
func WithNativeHistogramLimits(maxBuckets uint32, minReset time.Duration) Option {
	return func(c *config) {
		if maxBuckets > 0 {
			c.nativeMaxBuckets = maxBuckets
		}
		if minReset > 0 {
			c.nativeMinReset = minReset
		}
	}
}

// WithExemplars enables trace-linked exemplars on the duration histograms. When
// enabled, the context-aware observe methods (ObserveDecisionCtx,
// ObserveCBExecutionCtx) attach a trace_id / span_id exemplar to each histogram
// sample whenever the supplied context carries a sampled OpenTelemetry span.
//
// Exemplars are zero-cost when no sampled span is present in the context, and
// the context-free ObserveDecision / ObserveCBExecution methods (used by the
// zero-dependency core) never attach exemplars regardless of this option.
func WithExemplars(enabled bool) Option {
	return func(c *config) { c.exemplars = enabled }
}

// applyNative mutates opts to enable native histograms if configured. It leaves
// the classic Buckets untouched so both representations are exported.
func (c config) applyNative(opts *prometheus.HistogramOpts) {
	if !c.nativeHistograms {
		return
	}
	opts.NativeHistogramBucketFactor = c.nativeFactor
	opts.NativeHistogramMaxBucketNumber = c.nativeMaxBuckets
	opts.NativeHistogramMinResetDuration = c.nativeMinReset
}
