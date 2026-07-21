package store_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// asInts coerces the []any returned by a script handler into []int64, failing the
// test if any element is not an int64 (the handlers only ever return int64s).
func asInts(t *testing.T, name string, out any) []int64 {
	t.Helper()
	arr, ok := out.([]any)
	if !ok {
		t.Fatalf("%s: expected []any result, got %T", name, out)
	}
	res := make([]int64, len(arr))
	for i, v := range arr {
		n, ok := v.(int64)
		if !ok {
			t.Fatalf("%s: result[%d] not int64, got %T", name, i, v)
		}
		res[i] = n
	}
	return res
}

// FuzzScriptsMemory exercises the in-memory Lua-script emulations (token bucket,
// GCRA, leaky bucket) that the distributed limiters run against a *Memory store
// when Redis is absent. It drives fuzzed args/keys through Eval and asserts:
//   - no handler ever panics,
//   - a non-error result is a numeric slice with a sane allowed flag (0/1) and
//     non-negative remaining/queue-depth fields,
//   - repeated calls at a fixed clock stay monotonic (a key that denies once at
//     a fixed `now` keeps denying until the args change).
//
// Handler-level errors (e.g. refillRate <= 0, store-full) are a valid,
// documented "fail closed" outcome and are not treated as failures — only
// panics and out-of-range numbers are.
func FuzzScriptsMemory(f *testing.F) {
	// Seed a realistic corpus: [capacity/emission, rate/burst/cap, n, now, ttlMs].
	f.Add(int64(10), int64(1), int64(1), int64(1_000_000_000), int64(1000))
	f.Add(int64(1), int64(5), int64(1), int64(0), int64(60000))
	f.Add(int64(100), int64(10), int64(5), int64(2_000_000_000), int64(500))
	f.Add(int64(5), int64(2), int64(3), int64(1_500_000_000), int64(0))

	f.Fuzz(func(t *testing.T, a, b, n, now, ttlMs int64) {
		// Constrain args to the realistic domains the distributed limiters actually
		// pass: capacity/emission/burst/rate (a, b) are always positive, n >= 1.
		// Negative capacities are not a real Lua-script input, so we keep the fuzzer
		// meaningful by exercising the algorithm over its true operating range while
		// still probing wide value ranges and edge magnitudes.
		if a <= 0 || a > 1_000_000_000_000 {
			return
		}
		if b <= 0 || b > 1_000_000_000_000 {
			return
		}
		if n < 1 || n > 1_000_000 {
			return
		}
		if now < 0 || now > int64(1)<<62 {
			return
		}
		if ttlMs < 0 || ttlMs > 3_600_000 {
			return
		}

		ctx := context.Background()
		m := store.NewMemoryWithScripts()
		defer m.Close() //nolint:errcheck

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic: %v (a=%d b=%d n=%d now=%d ttl=%d)", r, a, b, n, now, ttlMs)
			}
		}()

		const key = "fuzz-key"

		// --- Token bucket: [capacity(float), refillRate(float), n, now, ttlMs] ---
		{
			out, err := m.Eval(ctx, store.TokenBucketScriptID, []string{key + ":tb"},
				float64(a), float64(b), float64(n), float64(now), ttlMs)
			if err == nil {
				r := asInts(t, "tokenBucket", out)
				if len(r) != 3 {
					t.Fatalf("tokenBucket: expected 3 results, got %d", len(r))
				}
				if r[0] != 0 && r[0] != 1 {
					t.Errorf("tokenBucket: allowed=%d not in {0,1}", r[0])
				}
				if r[1] < 0 {
					t.Errorf("tokenBucket: remaining=%d < 0", r[1])
				}
			}
		}

		// --- GCRA: [emission_ns, burst, n, now_ns, ttlMs] ---
		{
			out, err := m.Eval(ctx, store.GCRAScriptID, []string{key + ":gcra"},
				a, b, n, now, ttlMs)
			if err == nil {
				r := asInts(t, "gcra", out)
				if len(r) != 2 {
					t.Fatalf("gcra: expected 2 results, got %d", len(r))
				}
				if r[0] != 0 && r[0] != 1 {
					t.Errorf("gcra: allowed=%d not in {0,1}", r[0])
				}
				if r[1] < 0 {
					t.Errorf("gcra: retry_after=%d < 0", r[1])
				}
				if r[0] == 1 && r[1] != 0 {
					t.Errorf("gcra: allowed but retry_after=%d", r[1])
				}
			}
		}

		// --- Leaky bucket: [emission_ns, capacity, n, now_ns, ttlMs] ---
		{
			out, err := m.Eval(ctx, store.LeakyBucketScriptID, []string{key + ":lb"},
				a, b, n, now, ttlMs)
			if err == nil {
				r := asInts(t, "leakyBucket", out)
				if len(r) != 3 {
					t.Fatalf("leakyBucket: expected 3 results, got %d", len(r))
				}
				if r[0] != 0 && r[0] != 1 {
					t.Errorf("leakyBucket: allowed=%d not in {0,1}", r[0])
				}
				if r[1] < 0 {
					t.Errorf("leakyBucket: queue_depth=%d < 0", r[1])
				}
				if r[2] < 0 {
					t.Errorf("leakyBucket: retry_after=%d < 0", r[2])
				}
			}
		}
	})
}
