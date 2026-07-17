package adaptive

import (
	"math"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// SignalSource provides system health signals for adaptive rate limiting.
type SignalSource interface {
	// CPUPercent returns CPU utilization in range [0, 100].
	CPUPercent() float64

	// ErrorRate returns the recent error rate in range [0.0, 1.0].
	ErrorRate() float64

	// P99Latency returns the 99th percentile request latency.
	P99Latency() time.Duration
}

// ewma is an exponential weighted moving average seeded toward a neutral prior
// with a warmup window (L-7).
//
// A plain EMA that initializes value = firstSample lets a single early sample
// dominate: one error before any success reads as ErrorRate = 1.0, one success
// reads as 0.0. That over-reacts on cold start. Instead we seed value to a
// neutral prior and, for the first `warmup` samples, blend the running average
// with that prior so the estimate converges smoothly instead of snapping to the
// first observation.
type ewma struct {
	alpha  float64
	prior  float64 // neutral starting value
	warmup int     // number of initial samples during which the prior still pulls
	value  float64
	count  int
}

// newEWMAWithPrior creates an EWMA seeded at `prior` with a `warmup`-sample ramp.
func newEWMAWithPrior(alpha, prior float64, warmup int) *ewma {
	if warmup < 0 {
		warmup = 0
	}
	return &ewma{alpha: alpha, prior: prior, warmup: warmup, value: prior}
}

func (e *ewma) add(sample float64) {
	// Standard EMA update starting from the prior (not from the first sample).
	e.value = e.alpha*sample + (1-e.alpha)*e.value
	e.count++
	// During warmup, keep pulling the estimate toward the neutral prior with a
	// weight that decays as more samples arrive, so a lone early sample cannot
	// pin the estimate at 0 or 1.
	if e.count <= e.warmup && e.warmup > 0 {
		w := float64(e.count) / float64(e.warmup+1) // 0 < w < 1, rising toward 1
		e.value = w*e.value + (1-w)*e.prior
	}
}

func (e *ewma) get() float64 {
	return e.value
}

// histogram is a power-of-2 bucketed latency histogram (1µs to 10s).
type histogram struct {
	buckets [32]int64
}

func (h *histogram) record(d time.Duration) {
	ns := int64(d)
	if ns <= 0 {
		return
	}
	// Bucket index: floor(log2(ns / 1000)) clamped to [0, 31].
	// Use the stdlib intrinsic instead of a hand-rolled loop (L-7).
	bit := 64 - bits.LeadingZeros64(uint64(ns/1000))
	if bit < 0 {
		bit = 0
	}
	if bit >= 32 {
		bit = 31
	}
	h.buckets[bit]++
}

func (h *histogram) p99() time.Duration {
	total := int64(0)
	for _, c := range h.buckets {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := total * 99 / 100
	cumulative := int64(0)
	for i, c := range h.buckets {
		cumulative += c
		if cumulative >= target {
			// Convert bucket index back to duration (microseconds * 2^i)
			return time.Duration(1000 * (1 << uint(i)))
		}
	}
	return time.Duration(1000 * (1 << 31))
}

// cpuSampleInterval bounds how often CPUPercent performs a stop-the-world
// runtime.ReadMemStats. Between samples a cached value is returned so the
// adaptive hot path (called every Peek and every adjust cycle) never triggers a
// STW pause of its own (L-6).
const cpuSampleInterval = 250 * time.Millisecond

// RuntimeSignals uses Go runtime metrics as signals.
// ErrorRate uses an exponential moving average (α=0.1).
// P99 latency is tracked via an internal power-of-2 histogram.
type RuntimeSignals struct {
	mu          sync.Mutex
	errorRate   *ewma
	latencyHist histogram

	// CPU sample cache (L-6). cachedCPU/lastCPUSample are guarded by cpuMu so
	// CPUPercent does not contend on the record hot path's mu.
	cpuMu         sync.Mutex
	cachedCPU     float64
	lastCPUSample time.Time
	cpuInited     atomic.Bool
}

// NewRuntimeSignals creates a new RuntimeSignals with α=0.1 for error rate EMA.
// The error-rate EMA is seeded toward a neutral 0.5 prior with a warmup so a
// single early success/error does not read as a 0.0/1.0 error rate (L-7).
func NewRuntimeSignals() *RuntimeSignals {
	return &RuntimeSignals{
		errorRate: newEWMAWithPrior(0.1, 0.5, 10),
	}
}

// RecordSuccess records a successful request with its latency.
func (r *RuntimeSignals) RecordSuccess(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorRate.add(0.0)
	r.latencyHist.record(latency)
}

// RecordError records a failed request with its latency.
func (r *RuntimeSignals) RecordError(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorRate.add(1.0)
	r.latencyHist.record(latency)
}

// CPUPercent returns a crude proxy for CPU utilization, NOT a real measurement
// (L-6). Go has no dependency-free way to read true process/host CPU, so this
// blends two coarse runtime signals: the GC CPU fraction (from ReadMemStats) and
// a goroutine-count heuristic. Treat the result as a rough stress indicator, not
// an accurate CPU percentage — it can under- or over-report significantly.
//
// runtime.ReadMemStats stops the world, so it is sampled at most once per
// cpuSampleInterval; between samples a cached value is returned to keep the
// adaptive limiter's hot path (Peek/adjust) free of self-inflicted STW pauses.
func (r *RuntimeSignals) CPUPercent() float64 {
	now := time.Now()
	r.cpuMu.Lock()
	if r.cpuInited.Load() && now.Sub(r.lastCPUSample) < cpuSampleInterval {
		cached := r.cachedCPU
		r.cpuMu.Unlock()
		return cached
	}
	r.cpuMu.Unlock()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats) // STW — throttled by the cache above.
	// Use GC CPU fraction as a proxy; clamp to [0, 100]
	gcCPU := stats.GCCPUFraction * 100
	if gcCPU > 100 {
		gcCPU = 100
	}
	if gcCPU < 0 {
		gcCPU = 0
	}
	// Blend with goroutine count proxy
	goroutines := float64(runtime.NumGoroutine())
	gProxy := math.Min(goroutines/1000*100, 100)
	val := (gcCPU + gProxy) / 2

	r.cpuMu.Lock()
	r.cachedCPU = val
	r.lastCPUSample = now
	r.cpuInited.Store(true)
	r.cpuMu.Unlock()
	return val
}

// ErrorRate returns the EMA of recent request error rates.
func (r *RuntimeSignals) ErrorRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errorRate.get()
}

// P99Latency returns the 99th percentile latency from the histogram.
func (r *RuntimeSignals) P99Latency() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latencyHist.p99()
}

// staticSignals is a test-friendly SignalSource with fixed values.
type staticSignals struct {
	cpu       float64
	errorRate float64
	p99       time.Duration
}

// NewStaticSignals creates a SignalSource with fixed values (useful for testing).
func NewStaticSignals(cpu, errorRate float64, p99 time.Duration) SignalSource {
	return &staticSignals{cpu: cpu, errorRate: errorRate, p99: p99}
}

func (s *staticSignals) CPUPercent() float64       { return s.cpu }
func (s *staticSignals) ErrorRate() float64        { return s.errorRate }
func (s *staticSignals) P99Latency() time.Duration { return s.p99 }
