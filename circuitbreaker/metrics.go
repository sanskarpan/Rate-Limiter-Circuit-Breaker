package circuitbreaker

import (
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// WindowType determines how the circuit breaker tracks failures.
type WindowType int

const (
	// CountBased uses a ring buffer of the last N request outcomes.
	CountBased WindowType = iota
	// TimeBased uses a rolling time window with fixed-width buckets.
	TimeBased
)

// countWindow is a ring buffer of request outcomes.
// O(1) insert, O(1) query via precomputed failure count.
type countWindow struct {
	mu       sync.Mutex
	ring     []outcome // circular buffer
	head     int       // write index (next position to write)
	size     int       // configured capacity
	failures int       // precomputed failure count
	total    int       // min(writes, size) — number of valid entries
}

func newCountWindow(size int) *countWindow {
	return &countWindow{
		ring: make([]outcome, size),
		size: size,
	}
}

// record adds an outcome to the ring buffer and returns the window-wide
// (failures, total) counts as of this insertion. Returning the post-record
// counts lets the CLOSED hot path evaluate the open threshold without a second
// counts() mutex acquisition; both happen under the same lock, so the snapshot
// is consistent (§3.4).
func (w *countWindow) record(o outcome) (failures, total int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Evict the oldest entry if buffer is full
	if w.total == w.size {
		evicted := w.ring[w.head]
		if evicted == outcomeFailure {
			w.failures--
		}
	} else {
		w.total++
	}
	// Write new entry
	w.ring[w.head] = o
	if o == outcomeFailure {
		w.failures++
	}
	w.head = (w.head + 1) % w.size
	return w.failures, w.total
}

// counts returns (failures, total) in the window.
func (w *countWindow) counts() (failures, total int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failures, w.total
}

// reset clears all state.
func (w *countWindow) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ring = make([]outcome, w.size)
	w.head = 0
	w.failures = 0
	w.total = 0
}

// timeBucket is a single time-window bucket.
type timeBucket struct {
	start    time.Time
	failures int64
	requests int64
}

// timeWindow is a sliding time window implemented as fixed-width buckets.
// Buckets slide forward as time passes.
type timeWindow struct {
	mu             sync.Mutex
	buckets        []timeBucket
	numBuckets     int           // total number of buckets
	bucketWidth    time.Duration // width of each bucket
	windowDuration time.Duration // total retained span (numBuckets*bucketWidth)
	failures       int64         // precomputed total failures
	requests       int64         // precomputed total requests
	clock          clock.Clock
}

// maxTimeBuckets caps the number of buckets to keep memory bounded even if a
// pathological windowDuration/bucketWidth ratio is supplied.
const maxTimeBuckets = 4096

func newTimeWindow(windowDuration, bucketWidth time.Duration, clk clock.Clock) *timeWindow {
	// Guard against a zero/negative bucketWidth, which would otherwise panic on
	// the division below (L-10). Fall back to a single bucket spanning the window.
	if bucketWidth <= 0 {
		bucketWidth = windowDuration
	}
	if bucketWidth <= 0 {
		// windowDuration was also non-positive; use a sane default so newTimeWindow
		// never divides by zero and never produces a zero-width window.
		bucketWidth = time.Second
		windowDuration = time.Second
	}
	numBuckets := int(windowDuration / bucketWidth)
	if numBuckets < 1 {
		numBuckets = 1
	}
	if numBuckets > maxTimeBuckets {
		numBuckets = maxTimeBuckets
	}
	now := clk.Now()
	buckets := make([]timeBucket, numBuckets)
	// Lay the buckets out so the newest bucket starts at `now` and the oldest
	// starts at now-(numBuckets-1)*bucketWidth. Combined with the eviction rule
	// in slide (evict once now moves past oldest.start+windowDuration), the total
	// retained span equals exactly windowDuration rather than windowDuration+
	// bucketWidth (H-10).
	for i := range buckets {
		buckets[i].start = now.Add(-time.Duration(numBuckets-1-i) * bucketWidth)
	}
	return &timeWindow{
		buckets:        buckets,
		numBuckets:     numBuckets,
		bucketWidth:    bucketWidth,
		windowDuration: time.Duration(numBuckets) * bucketWidth,
		clock:          clk,
	}
}

// record adds an outcome to the current bucket, sliding as needed, and returns
// the window-wide (failures, requests) totals as of this insertion.
//
// Returning the post-record counts lets the CLOSED hot path evaluate the open
// threshold WITHOUT a second mutex acquisition (and second slide()) via a
// separate counts() call. Both the mutation and the read happen under the same
// lock, so the returned totals are a consistent snapshot — never torn against a
// concurrent record — and the threshold decision is exactly as correct as it was
// when it read counts() separately (§3.4).
func (w *timeWindow) record(o outcome) (failures, requests int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.slide()
	current := &w.buckets[w.numBuckets-1]
	current.requests++
	if o == outcomeFailure {
		current.failures++
		w.failures++
	}
	w.requests++
	return w.failures, w.requests
}

// slide evicts expired buckets and advances the window to now.
//
// The window keeps exactly numBuckets buckets, so the newest bucket starts at
// oldest.start+(numBuckets-1)*bucketWidth and the total span is windowDuration.
// The newest bucket is rolled forward (evicting the oldest and appending a new
// one) whenever `now` reaches or passes the end of the newest bucket. This keeps
// the retained span equal to windowDuration — a datapoint recorded at time t is
// evicted once now advances a full windowDuration past the start of t's bucket
// (H-10). The previous threshold retained windowDuration+bucketWidth.
func (w *timeWindow) slide() {
	now := w.clock.Now()
	for {
		newestStart := w.buckets[w.numBuckets-1].start
		// Roll forward once now reaches the end of the newest bucket.
		if now.Before(newestStart.Add(w.bucketWidth)) {
			break
		}
		// Evict oldest bucket
		oldest := &w.buckets[0]
		w.failures -= oldest.failures
		w.requests -= oldest.requests
		// Shift buckets left
		copy(w.buckets, w.buckets[1:])
		// Create new current bucket immediately after the previous newest.
		w.buckets[w.numBuckets-1] = timeBucket{
			start: newestStart.Add(w.bucketWidth),
		}
	}
}

// counts returns (failures, requests) across all non-expired buckets.
func (w *timeWindow) counts() (failures, requests int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.slide()
	return w.failures, w.requests
}

// reset clears all state.
func (w *timeWindow) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock.Now()
	for i := range w.buckets {
		w.buckets[i] = timeBucket{
			start: now.Add(-time.Duration(w.numBuckets-1-i) * w.bucketWidth),
		}
	}
	w.failures = 0
	w.requests = 0
}
