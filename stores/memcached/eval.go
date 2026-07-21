package memcached

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// ErrCASExhausted is returned (wrapped) when a client-side script cannot commit
// after maxCASRetries optimistic attempts. The distributed limiter callers treat
// any Eval error as a DENY, so the limiter fails CLOSED under pathological
// single-key contention rather than admitting on a stale computation.
var ErrCASExhausted = errors.New("memcached: CAS retry limit exhausted")

// ErrScriptUnsupported is returned by Eval for the Redis Lua scripts that cannot
// be made atomic on Memcached because they touch MULTIPLE keys with no cross-key
// transaction (sliding-window-counter, the distributed circuit breaker) or read
// the server clock in a way Memcached cannot provide. Use the Redis backend for
// those algorithms.
var ErrScriptUnsupported = errors.New("memcached: script not supported (requires multi-key atomicity or server-side scripting; use Redis)")

// Eval executes one of the known rate-limiting "scripts" client-side.
//
// Redis runs each script as an atomic Lua body on the server. Memcached has no
// server-side scripting, so each supported script is re-expressed here as an
// optimistic Gets → compute → CompareAndSwap loop over the script's SINGLE state
// key (see the package doc's atomicity model). The script parameter is matched
// against the exact script-body constants exported by the core store package
// (store.TokenBucketScript, etc.), identical to how the in-memory store's Eval
// dispatches by body — so the distributed limiters wire against this store with
// no changes.
//
// Supported (single-key, portable): TokenBucket, GCRA, LeakyBucket, FixedWindow,
// SlidingWindowLog.
//
// Unsupported (returns ErrScriptUnsupported): SlidingWindowCounter and all
// CircuitBreaker scripts — they require atomic access to more than one key, which
// Memcached cannot provide.
func (m *Memcached) Eval(ctx context.Context, scriptID store.ScriptID, keys []string, args ...any) (any, error) {
	switch scriptID {
	case store.TokenBucketScriptID:
		return m.evalTokenBucket(ctx, keys, args)
	case store.FixedWindowScriptID:
		return m.evalFixedWindow(ctx, keys, args)
	case store.GCRAScriptID:
		return m.evalGCRA(ctx, keys, args)
	case store.LeakyBucketScriptID:
		return m.evalLeakyBucket(ctx, keys, args)
	case store.SlidingWindowLogScriptID:
		return m.evalSlidingWindowLog(ctx, keys, args)
	case store.SlidingWindowCounterScriptID,
		store.CircuitBreakerAcquireScriptID,
		store.CircuitBreakerRecordScriptID,
		store.CircuitBreakerReadScriptID:
		return nil, ErrScriptUnsupported
	default:
		return nil, fmt.Errorf("memcached eval %q: %w", scriptID, ErrScriptUnsupported)
	}
}

// ---------------------------------------------------------------------------
// arg coercion (mirrors scripts_memory.go so behaviour matches the Redis/Memory
// backends bit-for-bit where the algorithm is float64-sensitive)
// ---------------------------------------------------------------------------

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		return 0, fmt.Errorf("memcached eval: cannot coerce %T to float64", v)
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("memcached eval: cannot coerce %T to int64", v)
	}
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// casMutate runs the generic optimistic loop over a single key. compute receives
// the current raw value ("" when the key is absent) and returns the new raw value
// to store, the memcached exptime seconds, whether to write at all, and the value
// to return to the caller. If write is false, the key is left untouched and result
// is returned immediately (used by deny paths that must not persist state).
func (m *Memcached) casMutate(
	ctx context.Context,
	scriptID store.ScriptID,
	key string,
	compute func(raw string, exists bool) (newVal string, exp int32, write bool, result any, err error),
) (any, error) {
	pk := m.prefixed(key)
	for i := 0; i < maxCASRetries; i++ {
		item, gerr := m.mc.Get(pk)
		exists := gerr == nil
		if gerr != nil && !errors.Is(gerr, memcache.ErrCacheMiss) {
			if isConnError(gerr) {
				return m.opts.Fallback.Eval(ctx, scriptID, []string{key})
			}
			return nil, fmt.Errorf("memcached eval get %q: %w", key, gerr)
		}
		raw := ""
		if exists {
			raw = string(item.Value)
		}
		newVal, exp, write, result, cerr := compute(raw, exists)
		if cerr != nil {
			return nil, cerr
		}
		if !write {
			return result, nil
		}
		if !exists {
			addErr := m.mc.Add(&memcache.Item{Key: pk, Value: []byte(newVal), Expiration: exp})
			if addErr == nil {
				return result, nil
			}
			if errors.Is(addErr, memcache.ErrNotStored) {
				continue // raced; retry as CAS
			}
			if isConnError(addErr) {
				return m.opts.Fallback.Eval(ctx, scriptID, []string{key})
			}
			return nil, fmt.Errorf("memcached eval add %q: %w", key, addErr)
		}
		item.Value = []byte(newVal)
		item.Expiration = exp
		casErr := m.mc.CompareAndSwap(item)
		if casErr == nil {
			return result, nil
		}
		if errors.Is(casErr, memcache.ErrCASConflict) || errors.Is(casErr, memcache.ErrNotStored) {
			continue
		}
		if isConnError(casErr) {
			return m.opts.Fallback.Eval(ctx, scriptID, []string{key})
		}
		return nil, fmt.Errorf("memcached eval cas %q: %w", key, casErr)
	}
	return nil, fmt.Errorf("memcached eval %q: %w", key, ErrCASExhausted)
}



// ---------------------------------------------------------------------------
// Token bucket (mirrors store.TokenBucketScript / tokenBucketHandler)
// State: "tokens|last_refill". Single key.
// Args: [capacity, refillRate(tokens/ns), n, now_ns, ttl_ms, use_server_time]
// Returns: []any{allowed int64, remaining int64, refilled int64}
// ---------------------------------------------------------------------------

func (m *Memcached) evalTokenBucket(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("memcached tokenBucket: missing key")
	}
	if len(args) < 4 {
		return nil, fmt.Errorf("memcached tokenBucket: expected >=4 args, got %d", len(args))
	}
	capacity, err := toFloat(args[0])
	if err != nil {
		return nil, err
	}
	refillRate, err := toFloat(args[1])
	if err != nil {
		return nil, err
	}
	n, err := toFloat(args[2])
	if err != nil {
		return nil, err
	}
	now, err := toFloat(args[3])
	if err != nil {
		return nil, err
	}
	if refillRate <= 0 {
		// Match Redis/Memory: fail closed rather than admit with no expiry.
		return nil, fmt.Errorf("memcached tokenBucket: refillRate must be > 0, got %v", refillRate)
	}
	var ttlMs int64
	if len(args) >= 5 {
		ttlMs, _ = toInt64(args[4])
	}
	if ttlMs < 1 {
		ttlMs = int64(math.Ceil(capacity / refillRate / 1000000))
	}
	exp := ttlToSeconds(time.Duration(ttlMs) * time.Millisecond)

	return m.casMutate(ctx, store.TokenBucketScriptID, keys[0], func(raw string, _ bool) (string, int32, bool, any, error) {
		tokens := capacity
		lastRefill := now
		if raw != "" {
			if t, lr, ok := parseTokenBucket(raw); ok {
				tokens = t
				lastRefill = lr
			}
		}
		elapsed := now - lastRefill
		refilled := elapsed * refillRate
		tokens = math.Min(capacity, tokens+refilled)
		lastRefill = now

		allowed := int64(0)
		if tokens >= n {
			tokens -= n
			allowed = 1
		}
		newVal := formatTokenBucket(tokens, lastRefill)
		result := []any{allowed, int64(math.Floor(tokens)), int64(math.Floor(refilled))}
		// Always persist the refilled state (matches Redis, which HMSETs on both
		// allow and deny).
		return newVal, exp, true, result, nil
	})
}

func parseTokenBucket(s string) (tokens, lastRefill float64, ok bool) {
	i := strings.IndexByte(s, '|')
	if i < 0 {
		return 0, 0, false
	}
	t, err1 := strconv.ParseFloat(s[:i], 64)
	lr, err2 := strconv.ParseFloat(s[i+1:], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return t, lr, true
}

func formatTokenBucket(tokens, lastRefill float64) string {
	return strconv.FormatFloat(tokens, 'g', -1, 64) + "|" + strconv.FormatFloat(lastRefill, 'g', -1, 64)
}

// ---------------------------------------------------------------------------
// Fixed window (mirrors store.FixedWindowScript / fixedWindowHandler)
// State: counter integer string. Single key.
// Args: [limit, n, ttl_ms]  Returns: []any{allowed int64, count int64}
// ---------------------------------------------------------------------------

func (m *Memcached) evalFixedWindow(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("memcached fixedWindow: missing key")
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("memcached fixedWindow: expected >=3 args, got %d", len(args))
	}
	limit, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	n, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	ttlMs, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	exp := ttlToSeconds(time.Duration(ttlMs) * time.Millisecond)

	return m.casMutate(ctx, store.FixedWindowScriptID, keys[0], func(raw string, _ bool) (string, int32, bool, any, error) {
		current := parseCounter(raw)
		if current+n > limit {
			// Deny: do NOT write (leaves TTL/count untouched, matching Redis).
			return "", 0, false, []any{int64(0), current}, nil
		}
		newCount := current + n
		return strconv.FormatInt(newCount, 10), exp, true, []any{int64(1), newCount}, nil
	})
}

func parseCounter(s string) int64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// ---------------------------------------------------------------------------
// GCRA (mirrors store.GCRAScript / gcraHandler)
// State: TAT nanosecond integer string. Single key.
// Args: [emission_interval_ns, burst, n, now_ns, ttl_ms, use_server_time]
// Returns: []any{allowed int64, retry_after_ns int64}
// Float64 arithmetic to reproduce Redis Lua double snapping.
// ---------------------------------------------------------------------------

func (m *Memcached) evalGCRA(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("memcached gcra: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("memcached gcra: expected >=5 args, got %d", len(args))
	}
	emission, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	burst, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	n, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	now, err := toInt64(args[3])
	if err != nil {
		return nil, err
	}
	ttlMs, err := toInt64(args[4])
	if err != nil {
		return nil, err
	}
	exp := ttlToSeconds(time.Duration(ttlMs) * time.Millisecond)

	return m.casMutate(ctx, store.GCRAScriptID, keys[0], func(raw string, _ bool) (string, int32, bool, any, error) {
		nowF := float64(now)
		storedF := nowF
		if raw != "" {
			if v, e := strconv.ParseInt(raw, 10, 64); e == nil {
				storedF = float64(v)
			}
		}
		baseF := storedF
		if nowF > baseF {
			baseF = nowF
		}
		tatF := baseF + float64(emission)*float64(n)
		limitWindowF := nowF + float64(emission)*float64(burst)
		if tatF > limitWindowF {
			// Deny: do not persist (matches Lua "only SET on allow").
			return "", 0, false, []any{int64(0), int64(tatF - limitWindowF)}, nil
		}
		return strconv.FormatInt(int64(tatF), 10), exp, true, []any{int64(1), int64(0)}, nil
	})
}

// ---------------------------------------------------------------------------
// Leaky bucket (mirrors store.LeakyBucketScript / leakyBucketHandler)
// State: TAT nanosecond integer string. Single key.
// Args: [emission_interval_ns, capacity, n, now_ns, ttl_ms, use_server_time]
// Returns: []any{allowed int64, queue_depth int64, retry_after_ns int64}
// ---------------------------------------------------------------------------

func (m *Memcached) evalLeakyBucket(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("memcached leakyBucket: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("memcached leakyBucket: expected >=5 args, got %d", len(args))
	}
	emission, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	capacity, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	n, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	now, err := toInt64(args[3])
	if err != nil {
		return nil, err
	}
	ttlMs, err := toInt64(args[4])
	if err != nil {
		return nil, err
	}
	exp := ttlToSeconds(time.Duration(ttlMs) * time.Millisecond)

	clampDepth := func(d float64) int64 {
		di := int64(math.Floor(d))
		if di < 0 {
			di = 0
		}
		if di > capacity {
			di = capacity
		}
		return di
	}

	return m.casMutate(ctx, store.LeakyBucketScriptID, keys[0], func(raw string, _ bool) (string, int32, bool, any, error) {
		nowF := float64(now)
		storedF := nowF
		if raw != "" {
			if v, e := strconv.ParseInt(raw, 10, 64); e == nil {
				storedF = float64(v)
			}
		}
		baseF := storedF
		if nowF > baseF {
			baseF = nowF
		}
		newTatF := baseF + float64(emission)*float64(n)
		limitWindowF := nowF + float64(emission)*float64(capacity)
		if newTatF > limitWindowF {
			retryAfter := int64(newTatF - limitWindowF)
			depth := clampDepth((baseF - nowF) / float64(emission))
			return "", 0, false, []any{int64(0), depth, retryAfter}, nil
		}
		depth := clampDepth((newTatF - nowF) / float64(emission))
		return strconv.FormatInt(int64(newTatF), 10), exp, true, []any{int64(1), depth, int64(0)}, nil
	})
}

// ---------------------------------------------------------------------------
// Sliding window log (mirrors store.SlidingWindowLogScript / slidingWindowLogHandler)
// State: serialized ZSET (score\x1fmember\x1e...). Single key.
// Args: [limit, window_ns, now_ns, entry_id, ttl_ms, n, use_server_time]
// Returns: []any{allowed int64, count int64, retry_after_ns int64}
// ---------------------------------------------------------------------------

func (m *Memcached) evalSlidingWindowLog(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("memcached slidingWindowLog: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("memcached slidingWindowLog: expected >=5 args, got %d", len(args))
	}
	limit, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	windowNs, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	nowNs, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	entryID := toStr(args[3])
	ttlMs, err := toInt64(args[4])
	if err != nil {
		return nil, err
	}
	n := int64(1)
	if len(args) >= 6 {
		if v, e := toInt64(args[5]); e == nil {
			n = v
		}
	}
	exp := ttlToSeconds(time.Duration(ttlMs) * time.Millisecond)
	// Deny path uses ceil(ttl_ms/1000)*1000 ms in the Redis script; convert.
	denyExp := ttlToSeconds(time.Duration(int64(math.Ceil(float64(ttlMs)/1000.0))*1000) * time.Millisecond)

	return m.casMutate(ctx, store.SlidingWindowLogScriptID, keys[0], func(raw string, _ bool) (string, int32, bool, any, error) {
		nowScore := int64(float64(nowNs))
		zs := parseZSet(raw)
		cutoff := int64(float64(nowNs) - float64(windowNs))
		zs.removeByScoreUpTo(cutoff)
		count := int64(len(zs.members))

		if count+n > limit {
			retryAfter := int64(0)
			if len(zs.members) > 0 {
				oldest := zs.members[0].score
				retryAfter = windowNs - (nowScore - oldest)
				if retryAfter < 0 {
					retryAfter = 0
				}
			}
			// Redis re-writes the pruned set on deny (EXPIRE + implicit ZREM
			// persistence). Persist the pruned set so eviction survives.
			return zs.serialize(), denyExp, true, []any{int64(0), count, retryAfter}, nil
		}
		for i := int64(1); i <= n; i++ {
			zs.add(nowScore, fmt.Sprintf("%s-%d", entryID, i))
		}
		return zs.serialize(), exp, true, []any{int64(1), count + n, int64(0)}, nil
	})
}
