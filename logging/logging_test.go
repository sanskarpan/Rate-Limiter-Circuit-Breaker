package logging_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/logging"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// newBufLogger returns a JSON slog.Logger writing into buf at the given level.
func newBufLogger(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})
	return slog.New(h)
}

// records parses each JSON line the handler wrote.
func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestNewRecorder_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		call    func(r metric.Recorder)
		wantMsg string
		wantKV  map[string]any
	}{
		{
			name:    "IncAllowed",
			call:    func(r metric.Recorder) { r.IncAllowed("tokenbucket") },
			wantMsg: "ratelimit decision",
			wantKV:  map[string]any{"algorithm": "tokenbucket", "allowed": true},
		},
		{
			name:    "IncDenied",
			call:    func(r metric.Recorder) { r.IncDenied("gcra") },
			wantMsg: "ratelimit decision",
			wantKV:  map[string]any{"algorithm": "gcra", "allowed": false},
		},
		{
			name:    "ObserveDecision",
			call:    func(r metric.Recorder) { r.ObserveDecision("tb", 5*time.Millisecond) },
			wantMsg: "ratelimit decision latency",
			wantKV:  map[string]any{"algorithm": "tb", "duration": float64(5 * time.Millisecond)},
		},
		{
			name:    "RecordCBState",
			call:    func(r metric.Recorder) { r.RecordCBState("pay", "open") },
			wantMsg: "circuitbreaker state",
			wantKV:  map[string]any{"name": "pay", "state": "open"},
		},
		{
			name:    "IncCBResult",
			call:    func(r metric.Recorder) { r.IncCBResult("pay", "failure") },
			wantMsg: "circuitbreaker result",
			wantKV:  map[string]any{"name": "pay", "result": "failure"},
		},
		{
			name:    "ObserveCBExecution",
			call:    func(r metric.Recorder) { r.ObserveCBExecution("pay", time.Second) },
			wantMsg: "circuitbreaker execution",
			wantKV:  map[string]any{"name": "pay", "duration": float64(time.Second)},
		},
		{
			name:    "IncCBTransition",
			call:    func(r metric.Recorder) { r.IncCBTransition("pay", "closed", "open") },
			wantMsg: "circuitbreaker transition",
			wantKV:  map[string]any{"name": "pay", "from": "closed", "to": "open"},
		},
		{
			name:    "SetBulkheadInflight",
			call:    func(r metric.Recorder) { r.SetBulkheadInflight("db", 3) },
			wantMsg: "bulkhead inflight",
			wantKV:  map[string]any{"name": "db", "inflight": float64(3)},
		},
		{
			name:    "IncBulkheadRejected",
			call:    func(r metric.Recorder) { r.IncBulkheadRejected("db") },
			wantMsg: "bulkhead rejected",
			wantKV:  map[string]any{"name": "db"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			// Use LevelDebug so decision/outcome/transition all emit.
			rec := logging.NewRecorder(newBufLogger(&buf, slog.LevelDebug))
			tt.call(rec)

			recs := records(t, &buf)
			if len(recs) != 1 {
				t.Fatalf("want 1 record, got %d (%q)", len(recs), buf.String())
			}
			r := recs[0]
			if r["msg"] != tt.wantMsg {
				t.Errorf("msg = %q, want %q", r["msg"], tt.wantMsg)
			}
			for k, v := range tt.wantKV {
				if r[k] != v {
					t.Errorf("field %q = %v (%T), want %v (%T)", k, r[k], r[k], v, v)
				}
			}
		})
	}
}

func TestNewRecorder_LevelGating(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Handler only allows Info+, decisions default to Debug -> suppressed.
	rec := logging.NewRecorder(newBufLogger(&buf, slog.LevelInfo))
	rec.IncAllowed("tb")             // debug, suppressed
	rec.RecordCBState("pay", "open") // transition -> info, emitted

	recs := records(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("want 1 record (only the info-level state), got %d: %q", len(recs), buf.String())
	}
	if recs[0]["msg"] != "circuitbreaker state" {
		t.Errorf("unexpected record: %v", recs[0])
	}
}

func TestNewRecorder_Options(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	rec := logging.NewRecorder(
		newBufLogger(&buf, slog.LevelDebug),
		logging.WithGroup("resilience"),
		logging.WithDecisionLevel(slog.LevelWarn),
	)
	rec.IncAllowed("tb")

	recs := records(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r["level"] != "WARN" {
		t.Errorf("level = %v, want WARN (decision level override)", r["level"])
	}
	grp, ok := r["resilience"].(map[string]any)
	if !ok {
		t.Fatalf("expected grouped attrs under 'resilience', got %v", r)
	}
	if grp["algorithm"] != "tb" {
		t.Errorf("grouped algorithm = %v, want tb", grp["algorithm"])
	}
}

func TestNewRecorder_NilLoggerUsesDefault(t *testing.T) {
	t.Parallel()
	// Should not panic with a nil logger.
	rec := logging.NewRecorder(nil)
	rec.IncAllowed("tb")
}

func TestCircuitBreakerHooks_Fields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	h := logging.NewCircuitBreakerHooks(newBufLogger(&buf, slog.LevelDebug))

	h.OnStateChange("pay", circuitbreaker.StateClosed, circuitbreaker.StateOpen)
	h.OnSuccess("pay", 2*time.Millisecond)
	h.OnFailure("pay", 3*time.Millisecond, errors.New("boom"))
	h.OnRejected("pay")

	recs := records(t, &buf)
	if len(recs) != 4 {
		t.Fatalf("want 4 records, got %d: %q", len(recs), buf.String())
	}

	if recs[0]["msg"] != "circuitbreaker transition" ||
		recs[0]["from"] != "closed" || recs[0]["to"] != "open" || recs[0]["name"] != "pay" {
		t.Errorf("transition record wrong: %v", recs[0])
	}
	if recs[1]["msg"] != "circuitbreaker success" || recs[1]["name"] != "pay" {
		t.Errorf("success record wrong: %v", recs[1])
	}
	if recs[2]["msg"] != "circuitbreaker failure" || recs[2]["error"] != "boom" {
		t.Errorf("failure record wrong: %v", recs[2])
	}
	if recs[3]["msg"] != "circuitbreaker rejected" {
		t.Errorf("rejected record wrong: %v", recs[3])
	}
}

// TestCircuitBreakerHooks_Assignable proves the hooks are directly assignable
// to circuitbreaker.Config fields (compile-time contract).
func TestCircuitBreakerHooks_Assignable(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	h := logging.NewCircuitBreakerHooks(newBufLogger(&buf, slog.LevelDebug))

	cfg := circuitbreaker.Config{
		Name:      "svc",
		OnSuccess: h.OnSuccess,
		OnFailure: h.OnFailure,
		OnRejected: func(name string) {
			h.OnRejected(name)
		},
		// OnStateChange has a circuitbreaker.State arg; adapt via closure since
		// State satisfies the fmt.Stringer-shaped param.
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			h.OnStateChange(name, from, to)
		},
	}
	cb := circuitbreaker.New(cfg)
	if cb == nil {
		t.Fatal("nil breaker")
	}
}

func TestCircuitBreakerHooks_NilState(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	h := logging.NewCircuitBreakerHooks(newBufLogger(&buf, slog.LevelDebug))
	// nil Stringer must not panic.
	h.OnStateChange("svc", nil, nil)
	recs := records(t, &buf)
	if len(recs) != 1 || recs[0]["from"] != "<nil>" {
		t.Fatalf("want nil-safe record, got %v", recs)
	}
}

func TestRecorder_ConcurrentUse(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	var mu sync.Mutex
	// slog handlers are concurrency-safe, but guard the buffer for the test.
	rec := logging.NewRecorder(slog.New(slog.NewJSONHandler(
		&syncWriter{buf: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug})))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec.IncAllowed("tb")
			rec.IncCBTransition("pay", "closed", "open")
		}()
	}
	wg.Wait()
}

type syncWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
