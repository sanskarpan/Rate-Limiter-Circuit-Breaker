package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// ErrOCCExhausted is returned (wrapped) when an optimistic-concurrency write
// cannot commit after maxOCCRetries attempts. Distributed limiter callers treat
// any Eval error as a DENY, so the limiter fails CLOSED under pathological
// single-item contention.
var ErrOCCExhausted = errors.New("dynamodb: optimistic-concurrency retry limit exhausted")

// ErrScriptUnsupported mirrors the memcached backend: the Redis Lua scripts that
// need atomic access to MULTIPLE items (sliding-window-counter, the circuit
// breaker) cannot be expressed as a single-item conditional write, so they are
// not supported here. Use the Redis backend for those algorithms.
var ErrScriptUnsupported = errors.New("dynamodb: script not supported (requires multi-item atomicity; use Redis)")

// Eval executes one of the known rate-limiting scripts as a single-item OCC
// read-compute-conditional-write loop, dispatched by the ScriptID constant.
//
// Supported (single-item, portable): TokenBucket, GCRA, LeakyBucket, FixedWindow,
// SlidingWindowLog. Unsupported: SlidingWindowCounter and the CircuitBreaker
// scripts (ErrScriptUnsupported).
func (d *DynamoDB) Eval(ctx context.Context, scriptID store.ScriptID, keys []string, args ...any) (any, error) {
	switch scriptID {
	case store.TokenBucketScriptID:
		return d.evalTokenBucket(ctx, keys, args)
	case store.FixedWindowScriptID:
		return d.evalFixedWindow(ctx, keys, args)
	case store.GCRAScriptID:
		return d.evalGCRA(ctx, keys, args)
	case store.LeakyBucketScriptID:
		return d.evalLeakyBucket(ctx, keys, args)
	case store.SlidingWindowLogScriptID:
		return d.evalSlidingWindowLog(ctx, keys, args)
	case store.SlidingWindowCounterScriptID,
		store.CircuitBreakerAcquireScriptID,
		store.CircuitBreakerRecordScriptID,
		store.CircuitBreakerReadScriptID:
		return nil, ErrScriptUnsupported
	default:
		return nil, fmt.Errorf("dynamodb eval %q: %w", scriptID, ErrScriptUnsupported)
	}
}

// putIfVersion writes attrs into the item conditionally on the version matching
// the base item's version (OCC). When base is nil (item absent) it writes
// conditional on absence. It always sets/increments the ver attribute and
// applies the TTL. Returns wrote=false on a version conflict so the caller
// retries.
func (d *DynamoDB) putIfVersion(
	ctx context.Context,
	key string,
	attrs map[string]ddbtypes.AttributeValue,
	base map[string]ddbtypes.AttributeValue,
	ttl time.Duration,
) (bool, error) {
	item := d.keyAttr(key)
	for k, v := range attrs {
		item[k] = v
	}
	if exp, ok := expAt(ttl); ok {
		item[attrExp] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)}
	}

	var cond string
	names := map[string]string{"#pk": d.opts.PartitionKey}
	values := map[string]ddbtypes.AttributeValue{}

	if base == nil {
		// Create: item must not exist (or be logically expired).
		cond = "attribute_not_exists(#pk) OR #exp < :now"
		names["#exp"] = attrExp
		values[":now"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Unix(), 10)}
		item[attrVer] = &ddbtypes.AttributeValueMemberN{Value: "1"}
	} else {
		baseVer, _ := numAttr(base, attrVer)
		cond = "#ver = :ver"
		names["#ver"] = attrVer
		values[":ver"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(baseVer, 10)}
		item[attrVer] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(baseVer+1, 10)}
	}

	_, err := d.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(d.opts.TableName),
		Item:                      item,
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err == nil {
		return true, nil
	}
	if isConditionalFailed(err) {
		return false, nil
	}
	return false, err
}

// occ runs the generic single-item OCC loop for a script over keys[0]. compute
// receives the current raw value string and returns the new raw value, whether to
// write, the TTL, and the result. When write is false the deny result is returned
// without a write (leaving state untouched, matching the Redis "only SET on
// allow" scripts).
func (d *DynamoDB) occ(
	ctx context.Context,
	scriptID store.ScriptID,
	key string,
	compute func(raw string) (newVal string, write bool, ttl time.Duration, result any, err error),
) (any, error) {
	for i := 0; i < maxOCCRetries; i++ {
		item, err := d.getItem(ctx, key)
		if err != nil {
			if isConnError(err) {
				return d.opts.Fallback.Eval(ctx, scriptID, []string{key})
			}
			return nil, fmt.Errorf("dynamodb eval get %q: %w", key, err)
		}
		raw := ""
		if item != nil {
			if s, ok := stringAttr(item, attrVal); ok {
				raw = s
			}
		}
		newVal, write, ttl, result, cerr := compute(raw)
		if cerr != nil {
			return nil, cerr
		}
		if !write {
			return result, nil
		}
		wrote, err := d.putIfVersion(ctx, key, map[string]ddbtypes.AttributeValue{
			attrVal: &ddbtypes.AttributeValueMemberS{Value: newVal},
		}, item, ttl)
		if err != nil {
			if isConnError(err) {
				return d.opts.Fallback.Eval(ctx, scriptID, []string{key})
			}
			return nil, fmt.Errorf("dynamodb eval put %q: %w", key, err)
		}
		if wrote {
			return result, nil
		}
		// Version conflict: retry.
	}
	return nil, fmt.Errorf("dynamodb eval %q: %w", key, ErrOCCExhausted)
}

// ---------------------------------------------------------------------------
// arg coercion (identical to the memcached/memory backends)
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
		return 0, fmt.Errorf("dynamodb eval: cannot coerce %T to float64", v)
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
		return 0, fmt.Errorf("dynamodb eval: cannot coerce %T to int64", v)
	}
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// ---------------------------------------------------------------------------
// Token bucket (mirrors store.TokenBucketScript)
// ---------------------------------------------------------------------------

func (d *DynamoDB) evalTokenBucket(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("dynamodb tokenBucket: missing key")
	}
	if len(args) < 4 {
		return nil, fmt.Errorf("dynamodb tokenBucket: expected >=4 args, got %d", len(args))
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
		return nil, fmt.Errorf("dynamodb tokenBucket: refillRate must be > 0, got %v", refillRate)
	}
	var ttlMs int64
	if len(args) >= 5 {
		ttlMs, _ = toInt64(args[4])
	}
	if ttlMs < 1 {
		ttlMs = int64(math.Ceil(capacity / refillRate / 1000000))
	}
	ttl := time.Duration(ttlMs) * time.Millisecond

	return d.occ(ctx, store.TokenBucketScriptID, keys[0], func(raw string) (string, bool, time.Duration, any, error) {
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
		result := []any{allowed, int64(math.Floor(tokens)), int64(math.Floor(refilled))}
		return formatTokenBucket(tokens, lastRefill), true, ttl, result, nil
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
// Fixed window (mirrors store.FixedWindowScript)
// ---------------------------------------------------------------------------

func (d *DynamoDB) evalFixedWindow(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("dynamodb fixedWindow: missing key")
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("dynamodb fixedWindow: expected >=3 args, got %d", len(args))
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
	ttl := time.Duration(ttlMs) * time.Millisecond

	return d.occ(ctx, store.FixedWindowScriptID, keys[0], func(raw string) (string, bool, time.Duration, any, error) {
		current := parseCounter(raw)
		if current+n > limit {
			return "", false, 0, []any{int64(0), current}, nil
		}
		newCount := current + n
		return strconv.FormatInt(newCount, 10), true, ttl, []any{int64(1), newCount}, nil
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
// GCRA (mirrors store.GCRAScript)
// ---------------------------------------------------------------------------

func (d *DynamoDB) evalGCRA(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("dynamodb gcra: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("dynamodb gcra: expected >=5 args, got %d", len(args))
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
	ttl := time.Duration(ttlMs) * time.Millisecond

	return d.occ(ctx, store.GCRAScriptID, keys[0], func(raw string) (string, bool, time.Duration, any, error) {
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
			return "", false, 0, []any{int64(0), int64(tatF - limitWindowF)}, nil
		}
		return strconv.FormatInt(int64(tatF), 10), true, ttl, []any{int64(1), int64(0)}, nil
	})
}

// ---------------------------------------------------------------------------
// Leaky bucket (mirrors store.LeakyBucketScript)
// ---------------------------------------------------------------------------

func (d *DynamoDB) evalLeakyBucket(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("dynamodb leakyBucket: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("dynamodb leakyBucket: expected >=5 args, got %d", len(args))
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
	ttl := time.Duration(ttlMs) * time.Millisecond

	clampDepth := func(dd float64) int64 {
		di := int64(math.Floor(dd))
		if di < 0 {
			di = 0
		}
		if di > capacity {
			di = capacity
		}
		return di
	}

	return d.occ(ctx, store.LeakyBucketScriptID, keys[0], func(raw string) (string, bool, time.Duration, any, error) {
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
			return "", false, 0, []any{int64(0), depth, retryAfter}, nil
		}
		depth := clampDepth((newTatF - nowF) / float64(emission))
		return strconv.FormatInt(int64(newTatF), 10), true, ttl, []any{int64(1), depth, int64(0)}, nil
	})
}

// ---------------------------------------------------------------------------
// Sliding window log (mirrors store.SlidingWindowLogScript)
// ---------------------------------------------------------------------------

func (d *DynamoDB) evalSlidingWindowLog(ctx context.Context, keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("dynamodb slidingWindowLog: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("dynamodb slidingWindowLog: expected >=5 args, got %d", len(args))
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
	ttl := time.Duration(ttlMs) * time.Millisecond
	denyTTL := time.Duration(int64(math.Ceil(float64(ttlMs)/1000.0))*1000) * time.Millisecond

	return d.occ(ctx, store.SlidingWindowLogScriptID, keys[0], func(raw string) (string, bool, time.Duration, any, error) {
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
			return zs.serialize(), true, denyTTL, []any{int64(0), count, retryAfter}, nil
		}
		for i := int64(1); i <= n; i++ {
			zs.add(nowScore, fmt.Sprintf("%s-%d", entryID, i))
		}
		return zs.serialize(), true, ttl, []any{int64(1), count + n, int64(0)}, nil
	})
}
