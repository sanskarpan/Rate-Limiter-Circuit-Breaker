package store

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// This file provides in-memory ScriptHandler emulations of the Lua scripts used
// by the distributed rate limiters (H-21 / TQ-1). Each handler implements the
// SAME algorithm as its corresponding Lua script in redis.go so that the
// Distributed* limiters can be exercised against a *Memory store with NO live
// Redis, and the tests are meaningful (they observe the real algorithm, not a
// stub). Keep these in sync with the Lua scripts.
//
// Register them all with RegisterDefaultScripts, or construct a ready-to-use
// store with NewMemoryWithScripts.

// RegisterDefaultScripts registers in-memory emulations for every named script
// the distributed limiters use, keyed by the exact script-body constants passed
// to Eval. After this call, DistributedTokenBucket, DistributedGCRA,
// DistributedSlidingWindowLog, DistributedSlidingWindowCounter, and
// DistributedFixedWindow all work against m without Redis.
func RegisterDefaultScripts(m *Memory) {
	m.RegisterScript(TokenBucketScript, m.tokenBucketHandler)
	m.RegisterScript(GCRAScript, m.gcraHandler)
	m.RegisterScript(SlidingWindowLogScript, m.slidingWindowLogHandler)
	m.RegisterScript(SlidingWindowCounterScript, m.slidingWindowCounterHandler)
	m.RegisterScript(FixedWindowScript, m.fixedWindowHandler)
	m.RegisterScript(CircuitBreakerAcquireScript, m.cbAcquireHandler)
	m.RegisterScript(CircuitBreakerRecordScript, m.cbRecordHandler)
	m.RegisterScript(CircuitBreakerReadScript, m.cbReadHandler)
}

// NewMemoryWithScripts creates a Memory store with all default script handlers
// registered. Convenience wrapper over NewMemory + RegisterDefaultScripts.
func NewMemoryWithScripts(opts ...MemoryOption) *Memory {
	m := NewMemory(opts...)
	RegisterDefaultScripts(m)
	return m
}

// ---------------------------------------------------------------------------
// arg coercion helpers
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
		return 0, fmt.Errorf("store: cannot coerce %T to float64", v)
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
		return 0, fmt.Errorf("store: cannot coerce %T to int64", v)
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

// serverTimeFlag returns true when the arg at idx (if present) coerces to "1",
// matching the Lua `tostring(use_server_time) == "1"` check. Absent/short arg
// lists default to false (client-time mode, backward compatible).
func serverTimeFlag(args []any, idx int) bool {
	if idx < 0 || idx >= len(args) {
		return false
	}
	return toString(args[idx]) == "1"
}

// memoryServerNowNs mirrors the Redis in-script TIME override for the in-memory
// emulation: when server-time mode is requested AND enabled on this store, the
// store's own local clock is the authoritative "server" clock, so the emulation
// substitutes time.Now() for the client-supplied `now`. It intentionally snaps
// to microsecond resolution to match Redis TIME (seconds + microseconds), then
// scales back to nanoseconds, so the emulation and the real script agree.
func (m *Memory) memoryServerNowNs(useServerTime bool) (int64, bool) {
	if !useServerTime || !m.useServerTime {
		return 0, false
	}
	return time.Now().UnixMicro() * 1000, true
}

// ---------------------------------------------------------------------------
// Token bucket emulation (mirrors TokenBucketScript)
// ---------------------------------------------------------------------------
//
// State per key is serialized as "tokens|last_refill".
// Args: [capacity(float), refillRate(float, tokens/ns), n(int), now(ns), ttl_ms(int)]
// Returns: []any{allowed int64, remaining int64, refilled int64}.
func (m *Memory) tokenBucketHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("tokenBucketHandler: missing key")
	}
	if len(args) < 4 {
		return nil, fmt.Errorf("tokenBucketHandler: expected >=4 args, got %d", len(args))
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
	// Server-time mode: use the store's own clock as the authoritative "server"
	// clock, ignoring the (possibly skewed) client-supplied now. Mirrors the Lua
	// TIME override guarded by use_server_time (ARGV[6], idx 5).
	if srv, ok := m.memoryServerNowNs(serverTimeFlag(args, 5)); ok {
		now = float64(srv)
	}
	// Mirror Redis: with refillRate <= 0 the Lua PEXPIRE argument becomes
	// capacity/0 = inf, which errors and fails the whole Eval (deny). Fail
	// closed here too rather than silently admitting with no expiry (F-5).
	if refillRate <= 0 {
		return nil, fmt.Errorf("tokenBucketHandler: refillRate must be > 0, got %v", refillRate)
	}
	var ttlMs int64
	if len(args) >= 5 {
		ttlMs, _ = toInt64(args[4])
	}
	if ttlMs < 1 {
		ttlMs = int64(math.Ceil(capacity / refillRate / 1000000))
	}

	key := keys[0]
	e, err := m.loadOrCreateRaw(key)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	tokens := capacity
	lastRefill := now
	if e.value != "" {
		if t, lr, ok := parseTokenBucket(e.value); ok {
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
	e.value = formatTokenBucket(tokens, lastRefill)
	m.setEntryTTLmsAbs(e, ttlMs)

	return []any{allowed, int64(math.Floor(tokens)), int64(math.Floor(refilled))}, nil
}

func parseTokenBucket(s string) (tokens, lastRefill float64, ok bool) {
	var a, b string
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			a = s[:i]
			b = s[i+1:]
			ok = true
			break
		}
	}
	if !ok {
		return 0, 0, false
	}
	t, err1 := strconv.ParseFloat(a, 64)
	lr, err2 := strconv.ParseFloat(b, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return t, lr, true
}

func formatTokenBucket(tokens, lastRefill float64) string {
	return strconv.FormatFloat(tokens, 'g', -1, 64) + "|" + strconv.FormatFloat(lastRefill, 'g', -1, 64)
}

// ---------------------------------------------------------------------------
// GCRA emulation (mirrors GCRAScript)
// ---------------------------------------------------------------------------
//
// State per key is the TAT as a nanosecond integer string.
// Args: [emission_interval_ns(int), burst(int), n(int), now_ns(int), ttl_ms(int)]
// Returns: []any{allowed int64, retry_after_ns int64}.
func (m *Memory) gcraHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("gcraHandler: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("gcraHandler: expected >=5 args, got %d", len(args))
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
	// Server-time mode: substitute the store's own clock for the client `now`
	// (mirrors the Lua TIME override guarded by use_server_time, ARGV[6], idx 5).
	if srv, ok := m.memoryServerNowNs(serverTimeFlag(args, 5)); ok {
		now = srv
	}

	key := keys[0]
	e, err := m.loadOrCreateRaw(key)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// Redis evaluates this in Lua, where the nanosecond TAT/now (~1.78e18, well
	// above float64's 2^53 exact-integer ceiling) are reloaded and computed as
	// doubles, snapping to ~256ns granularity. We do the arithmetic in float64
	// too so the in-memory emulation stays faithful to the real Lua script and
	// the memory-store tests validate the same algorithm Redis runs (F-1).
	nowF := float64(now)
	storedF := nowF
	if e.value != "" {
		if v, err := strconv.ParseInt(e.value, 10, 64); err == nil {
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
		// Denied — do NOT persist the tentative TAT (matches Lua: only SET on allow).
		return []any{int64(0), int64(tatF - limitWindowF)}, nil
	}
	// int64(tatF) is exact and round-trips back through float64 (tatF is a
	// whole-number double), matching Redis GET/tonumber on the next call.
	e.value = strconv.FormatInt(int64(tatF), 10)
	m.setEntryTTLmsAbs(e, ttlMs)
	return []any{int64(1), int64(0)}, nil
}

// ---------------------------------------------------------------------------
// Sliding window log emulation (mirrors SlidingWindowLogScript)
// ---------------------------------------------------------------------------
//
// State per key is a ZSET emulation: a slice of scored members serialized. We
// store it as a newline-joined list of "score member" pairs.
// Args: [limit(int), window_ns(int), now_ns(int), entry_id(string), ttl_ms(int), n(int)]
// Returns: []any{allowed int64, count int64, retry_after_ns int64}.
func (m *Memory) slidingWindowLogHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("slidingWindowLogHandler: missing key")
	}
	if len(args) < 5 {
		return nil, fmt.Errorf("slidingWindowLogHandler: expected >=5 args, got %d", len(args))
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
	entryID := toString(args[3])
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
	// Server-time mode: substitute the store's own clock for the client now_ns
	// (mirrors the Lua TIME override guarded by use_server_time, ARGV[7], idx 6).
	if srv, ok := m.memoryServerNowNs(serverTimeFlag(args, 6)); ok {
		nowNs = srv
	}

	key := keys[0]
	e, err := m.loadOrCreateRaw(key)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// Redis sorted-set scores are float64. A nanosecond score (~1.78e18) snaps to
	// ~256ns granularity when Redis stores/compares it, whereas exact int64 would
	// not. Round now/cutoff through float64 so the emulation's eviction and count
	// match the real ZSET behaviour (F-2).
	nowScore := int64(float64(nowNs))
	zs := parseZSet(e.value)
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
		e.value = zs.serialize()
		m.setEntryTTLmsAbs(e, int64(math.Ceil(float64(ttlMs)/1000.0))*1000)
		return []any{int64(0), count, retryAfter}, nil
	}
	for i := int64(1); i <= n; i++ {
		zs.add(nowScore, fmt.Sprintf("%s-%d", entryID, i))
	}
	e.value = zs.serialize()
	m.setEntryTTLmsAbs(e, ttlMs)
	return []any{int64(1), count + n, int64(0)}, nil
}

// ---------------------------------------------------------------------------
// Sliding window counter emulation (mirrors SlidingWindowCounterScript)
// ---------------------------------------------------------------------------
//
// Keys: [current_key, prev_key]
// Args: [limit(int), n(int), fraction_millionths(int), current_ttl_ms(int)]
// Returns: []any{allowed int64, new_current int64, estimated_scaled int64}.
func (m *Memory) slidingWindowCounterHandler(keys []string, args []any) (any, error) {
	if len(keys) < 2 {
		return nil, fmt.Errorf("slidingWindowCounterHandler: expected 2 keys, got %d", len(keys))
	}
	if len(args) < 4 {
		return nil, fmt.Errorf("slidingWindowCounterHandler: expected >=4 args, got %d", len(args))
	}
	limit, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	n, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	fracMillionths, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	currentTTLms, err := toInt64(args[3])
	if err != nil {
		return nil, err
	}
	fraction := float64(fracMillionths) / 1000000.0

	currentKey := keys[0]
	prevKey := keys[1]

	// Lock ordering: current before prev, deterministic to avoid deadlock.
	ce, err := m.loadOrCreateRaw(currentKey)
	if err != nil {
		return nil, err
	}
	pe := m.loadEntry(prevKey)

	// Hold BOTH the current and previous entry locks for the whole handler so the
	// two-window read + increment is an atomic snapshot, matching Redis's
	// single-threaded Lua evaluation (F-3). Lock order is always current-then-
	// previous (descending window epoch), which is globally consistent and
	// deadlock-free.
	ce.mu.Lock()
	defer ce.mu.Unlock()
	if pe != nil {
		pe.mu.Lock()
		defer pe.mu.Unlock()
	}

	current := parseCounter(ce.value)
	var prev int64
	if pe != nil {
		prev = parseCounter(pe.value)
	}

	estimated := float64(current) + float64(prev)*(1.0-fraction)
	estimatedScaled := int64(math.Floor(estimated * 1000000))

	if estimated+float64(n) > float64(limit) {
		return []any{int64(0), current, estimatedScaled}, nil
	}

	newCurrent := current + n
	ce.value = strconv.FormatInt(newCurrent, 10)
	if current == 0 {
		// Created this window: set TTL from its start.
		m.setEntryTTLmsAbs(ce, currentTTLms)
	}
	return []any{int64(1), newCurrent, estimatedScaled}, nil
}

// ---------------------------------------------------------------------------
// Fixed window emulation (mirrors FixedWindowScript)
// ---------------------------------------------------------------------------
//
// Keys: [window_key]
// Args: [limit(int), n(int), ttl_ms(int)]
// Returns: []any{allowed int64, count int64}.
func (m *Memory) fixedWindowHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("fixedWindowHandler: missing key")
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("fixedWindowHandler: expected >=3 args, got %d", len(args))
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

	key := keys[0]
	e, err := m.loadOrCreateRaw(key)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	current := parseCounter(e.value)
	if current+n > limit {
		return []any{int64(0), current}, nil
	}
	newCount := current + n
	e.value = strconv.FormatInt(newCount, 10)
	if current == 0 {
		m.setEntryTTLmsAbs(e, ttlMs)
	}
	return []any{int64(1), newCount}, nil
}

// ---------------------------------------------------------------------------
// low-level entry helpers for the handlers
// ---------------------------------------------------------------------------

// errStoreFull is returned by loadOrCreateRaw when the store is at maxKeys and
// the script targets a brand-new key. Handlers propagate it, and the distributed
// limiter callers treat an Eval error as a DENY — so we fail CLOSED rather than
// running the algorithm against a throwaway detached entry that always reads 0
// and therefore always admits (F-4).
var errStoreFull = fmt.Errorf("store: max keys limit exceeded, denying new key")

// loadOrCreateRaw returns the entry for key, creating an empty (never-expiring)
// one if absent, honoring maxKeys accounting and lazy expiry. Newly created or
// lazily-reset entries have an empty value so callers can detect "fresh". It
// returns errStoreFull when the store is at maxKeys and key is new (fail closed).
func (m *Memory) loadOrCreateRaw(key string) (*entry, error) {
	if v, ok := m.entries.Load(key); ok {
		e := v.(*entry)
		e.mu.Lock()
		if e.isExpired(time.Now()) {
			// Reset in place so the handler sees a fresh key without a
			// removal/re-add race. Entry stays counted.
			e.value = ""
			e.expiresAt = time.Time{}
		}
		e.mu.Unlock()
		return e, nil
	}
	// Reserve before publishing (M-14 discipline).
	if m.maxKeys > 0 {
		if err := m.reserveSlot(); err != nil {
			// At capacity for a NEW key: fail closed (deny) instead of
			// admitting everything (F-4).
			return nil, errStoreFull
		}
	}
	ne := &entry{}
	actual, loaded := m.entries.LoadOrStore(key, ne)
	if loaded {
		if m.maxKeys > 0 {
			m.keyCount.Add(-1)
		}
		return actual.(*entry), nil
	}
	return ne, nil
}

// loadEntry returns the entry for key or nil, treating expired as absent.
func (m *Memory) loadEntry(key string) *entry {
	v, ok := m.entries.Load(key)
	if !ok {
		return nil
	}
	e := v.(*entry)
	e.mu.Lock()
	expired := e.isExpired(time.Now())
	e.mu.Unlock()
	if expired {
		return nil
	}
	return e
}

// setEntryTTLmsAbs sets an entry's expiry ttlMs milliseconds from the real now.
func (m *Memory) setEntryTTLmsAbs(e *entry, ttlMs int64) {
	if ttlMs <= 0 {
		return
	}
	e.expiresAt = time.Now().Add(time.Duration(ttlMs) * time.Millisecond)
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
// minimal ZSET emulation for the sliding window log
// ---------------------------------------------------------------------------

type zmember struct {
	score  int64
	member string
}

type zset struct {
	members []zmember // kept sorted by score ascending
}

func parseZSet(s string) *zset {
	z := &zset{}
	if s == "" {
		return z
	}
	// Format: "score\x1fmember\x1e..." records separated by \x1e, fields by \x1f.
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\x1e' {
			rec := s[start:i]
			start = i + 1
			if rec == "" {
				continue
			}
			sep := -1
			for j := 0; j < len(rec); j++ {
				if rec[j] == '\x1f' {
					sep = j
					break
				}
			}
			if sep < 0 {
				continue
			}
			sc, err := strconv.ParseInt(rec[:sep], 10, 64)
			if err != nil {
				continue
			}
			z.members = append(z.members, zmember{score: sc, member: rec[sep+1:]})
		}
	}
	return z
}

func (z *zset) serialize() string {
	if len(z.members) == 0 {
		return ""
	}
	// Rough capacity estimate.
	buf := make([]byte, 0, len(z.members)*24)
	for i, mem := range z.members {
		if i > 0 {
			buf = append(buf, '\x1e')
		}
		buf = strconv.AppendInt(buf, mem.score, 10)
		buf = append(buf, '\x1f')
		buf = append(buf, mem.member...)
	}
	return string(buf)
}

// add inserts or updates a member with a given score (ZADD semantics: same
// member name updates its score). Members are kept sorted by score.
func (z *zset) add(score int64, member string) {
	for i := range z.members {
		if z.members[i].member == member {
			z.members[i].score = score
			z.resort()
			return
		}
	}
	z.members = append(z.members, zmember{score: score, member: member})
	z.resort()
}

func (z *zset) resort() {
	// Insertion of at most one changed element; simple stable-ish sort.
	for i := 1; i < len(z.members); i++ {
		for j := i; j > 0 && z.members[j-1].score > z.members[j].score; j-- {
			z.members[j-1], z.members[j] = z.members[j], z.members[j-1]
		}
	}
}

// removeByScoreUpTo removes all members with score <= cutoff (ZREMRANGEBYSCORE
// -inf cutoff).
func (z *zset) removeByScoreUpTo(cutoff int64) {
	idx := 0
	for idx < len(z.members) && z.members[idx].score <= cutoff {
		idx++
	}
	if idx > 0 {
		z.members = z.members[idx:]
	}
}

// ---------------------------------------------------------------------------
// Distributed circuit breaker emulation (mirrors CircuitBreaker*Script)
// ---------------------------------------------------------------------------
//
// State per breaker key is serialized as a fixed 5-field record:
//
//	"state|failures|successes|opened_at|half_open_inflight"
//
// This mirrors the Redis hash used by the Lua scripts. The Redis scripts read the
// server clock via TIME; the in-memory emulation instead reads `now_ns` from the
// FINAL ARGV element supplied by the DistributedCircuitBreaker (from its injected
// clock). Redis's Lua only indexes the ARGV positions it names, so passing this
// trailing arg is harmless there while making the memory emulation deterministic
// under a ManualClock. Keep these handlers in sync with the Lua scripts.

type cbState struct {
	state     int64
	failures  int64
	successes int64
	openedAt  int64
	inflight  int64
}

func parseCBState(s string) cbState {
	var st cbState
	if s == "" {
		return st
	}
	parts := make([]int64, 0, 5)
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '|' {
			v, err := strconv.ParseInt(s[start:i], 10, 64)
			if err != nil {
				v = 0
			}
			parts = append(parts, v)
			start = i + 1
		}
	}
	if len(parts) >= 5 {
		st.state, st.failures, st.successes, st.openedAt, st.inflight =
			parts[0], parts[1], parts[2], parts[3], parts[4]
	}
	return st
}

func (st cbState) serialize() string {
	return strconv.FormatInt(st.state, 10) + "|" +
		strconv.FormatInt(st.failures, 10) + "|" +
		strconv.FormatInt(st.successes, 10) + "|" +
		strconv.FormatInt(st.openedAt, 10) + "|" +
		strconv.FormatInt(st.inflight, 10)
}

// cbNowNs extracts the trailing now_ns arg (the DistributedCircuitBreaker's
// clock). Falls back to time.Now() so the emulation still works if omitted.
func cbNowNs(args []any) int64 {
	if len(args) == 0 {
		return time.Now().UnixNano()
	}
	v, err := toInt64(args[len(args)-1])
	if err != nil || v <= 0 {
		return time.Now().UnixNano()
	}
	return v
}

// cbAcquireHandler mirrors CircuitBreakerAcquireScript.
// Args: [open_timeout_ns, half_open_max, ttl_ms, now_ns]
// Returns: []any{decision int64, state int64}.
func (m *Memory) cbAcquireHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("cbAcquireHandler: missing key")
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("cbAcquireHandler: expected >=3 args, got %d", len(args))
	}
	openTimeout, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	halfOpenMax, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	ttlMs, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	now := cbNowNs(args)

	e, err := m.loadOrCreateRaw(keys[0])
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	st := parseCBState(e.value)

	if st.state == 2 {
		if st.openedAt <= 0 || (now-st.openedAt) < openTimeout {
			return []any{int64(1), int64(2)}, nil // reject: still open
		}
		// OpenTimeout elapsed: promote Open -> HalfOpen.
		st.state = 1
		st.successes = 0
		st.inflight = 0
	}

	if st.state == 1 {
		if st.inflight >= halfOpenMax {
			e.value = st.serialize()
			m.setEntryTTLmsAbs(e, ttlMs)
			return []any{int64(2), int64(1)}, nil // reject: probe limit
		}
		st.inflight++
		e.value = st.serialize()
		m.setEntryTTLmsAbs(e, ttlMs)
		return []any{int64(0), int64(1)}, nil // allow (probe reserved)
	}

	// Closed.
	e.value = st.serialize()
	m.setEntryTTLmsAbs(e, ttlMs)
	return []any{int64(0), int64(0)}, nil
}

// cbRecordHandler mirrors CircuitBreakerRecordScript.
// Args: [outcome, failure_threshold, success_threshold, open_timeout_ns, ttl_ms,
//
//	acquired_probe, now_ns]
//
// Returns: []any{state int64}.
func (m *Memory) cbRecordHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("cbRecordHandler: missing key")
	}
	if len(args) < 6 {
		return nil, fmt.Errorf("cbRecordHandler: expected >=6 args, got %d", len(args))
	}
	outcome, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	failureThreshold, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	successThreshold, err := toInt64(args[2])
	if err != nil {
		return nil, err
	}
	// args[3] open_timeout_ns and args[4] ttl_ms; open_timeout unused on record.
	ttlMs, err := toInt64(args[4])
	if err != nil {
		return nil, err
	}
	acquiredProbe, err := toInt64(args[5])
	if err != nil {
		return nil, err
	}
	now := cbNowNs(args)

	e, err := m.loadOrCreateRaw(keys[0])
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	st := parseCBState(e.value)

	if acquiredProbe == 1 && st.inflight > 0 {
		st.inflight--
	}

	openCircuit := func() {
		st.state = 2
		st.failures = 0
		st.successes = 0
		st.inflight = 0
		st.openedAt = now
	}
	closeCircuit := func() {
		st.state = 0
		st.failures = 0
		st.successes = 0
		st.inflight = 0
		st.openedAt = 0
	}

	switch st.state {
	case 1: // half-open
		if outcome == 1 {
			openCircuit()
		} else {
			st.successes++
			if st.successes >= successThreshold {
				closeCircuit()
			}
		}
	case 0: // closed
		if outcome == 1 {
			st.failures++
			if st.failures >= failureThreshold {
				openCircuit()
			}
		}
	}

	e.value = st.serialize()
	m.setEntryTTLmsAbs(e, ttlMs)
	return []any{st.state}, nil
}

// cbReadHandler mirrors CircuitBreakerReadScript (read-only).
// Args: [open_timeout_ns, now_ns]
// Returns: []any{state, failures, successes, opened_at, half_open_inflight}.
func (m *Memory) cbReadHandler(keys []string, args []any) (any, error) {
	if len(keys) < 1 {
		return nil, fmt.Errorf("cbReadHandler: missing key")
	}
	if len(args) < 1 {
		return nil, fmt.Errorf("cbReadHandler: expected >=1 arg, got %d", len(args))
	}
	openTimeout, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	now := cbNowNs(args)

	e := m.loadEntry(keys[0])
	if e == nil {
		return []any{int64(0), int64(0), int64(0), int64(0), int64(0)}, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	st := parseCBState(e.value)
	if st.state == 2 && st.openedAt > 0 && (now-st.openedAt) >= openTimeout {
		st.state = 1
	}
	return []any{st.state, st.failures, st.successes, st.openedAt, st.inflight}, nil
}
