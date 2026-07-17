// Package store provides in-memory and Redis-backed store implementations.
// This file contains the Redis store adapter using go-redis/v9.
//
// Redis is an OPTIONAL dependency — the core rate limiting algorithms work entirely
// with the in-memory store. Use the Redis store only for distributed deployments.
package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisOptions configures the Redis store.
type RedisOptions struct {
	// Addr is the Redis server address (default: localhost:6379).
	Addr string

	// Password for Redis AUTH (optional).
	Password string

	// DB is the Redis database number (default: 0).
	DB int

	// PoolSize is the maximum number of connections in the pool (default: 10).
	PoolSize int

	// MinIdleConns is the minimum number of idle connections (default: 2).
	MinIdleConns int

	// DialTimeout is the timeout for establishing new connections (default: 5s).
	DialTimeout time.Duration

	// ReadTimeout is the read timeout (default: 3s).
	ReadTimeout time.Duration

	// WriteTimeout is the write timeout (default: 3s).
	WriteTimeout time.Duration

	// MaxRetries on transient connection failures (default: 3).
	MaxRetries int

	// MinRetryBackoff before retrying (default: 8ms).
	MinRetryBackoff time.Duration

	// MaxRetryBackoff maximum backoff (default: 512ms).
	MaxRetryBackoff time.Duration

	// Fallback is the store to use when Redis is unavailable.
	//
	// FAIL-OPEN WARNING: If nil, NewRedis/NewRedisFromClient install a fresh,
	// per-process in-memory store as the fallback. When Redis is unreachable,
	// each application instance then rate-limits against its own local counters
	// with no shared state, so the effective global limit is multiplied by the
	// number of instances (fail-open / per-instance divergence). This preserves
	// availability during a Redis outage at the cost of enforcement accuracy.
	// Supply an explicit Fallback (or a fail-closed deny-all store) if that
	// trade-off is unacceptable for your deployment.
	Fallback Store

	// KeyPrefix is prepended to all keys (default: "rl:").
	KeyPrefix string

	// UseServerTime, when true, makes the time-sensitive distributed limiters
	// (token bucket, GCRA, sliding-window-log) use the Redis server's own clock
	// (via the in-script TIME command) as the authoritative time source instead
	// of the client-supplied `now`. This mitigates clock skew across an
	// application fleet: with skewed app clocks, client-time mode can corrupt a
	// distributed decision, whereas server-time mode pins every instance to the
	// single Redis clock.
	//
	// Default false (client time) for backward compatibility. Enabling it adds a
	// tiny per-call cost (an extra in-script TIME read) and forces effects
	// replication for those scripts. Note that FixedWindow and
	// SlidingWindowCounter derive their window boundaries in the Go caller, so
	// they are unaffected by this flag.
	//
	// This flag is read by ServerTimeMode(); distributed limiters wired against
	// this store can call that to decide whether to send use_server_time=1.
	UseServerTime bool
}

func (o *RedisOptions) defaults() {
	if o.Addr == "" {
		o.Addr = "localhost:6379"
	}
	if o.PoolSize == 0 {
		o.PoolSize = 10
	}
	if o.MinIdleConns == 0 {
		o.MinIdleConns = 2
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 5 * time.Second
	}
	if o.ReadTimeout == 0 {
		o.ReadTimeout = 3 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 3 * time.Second
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = 3
	}
	if o.MinRetryBackoff == 0 {
		o.MinRetryBackoff = 8 * time.Millisecond
	}
	if o.MaxRetryBackoff == 0 {
		o.MaxRetryBackoff = 512 * time.Millisecond
	}
	if o.Fallback == nil {
		o.Fallback = NewMemory()
	}
	if o.KeyPrefix == "" {
		o.KeyPrefix = "rl:"
	}
}

// Redis is a Store implementation backed by Redis.
//
// When Redis is unavailable (connection refused, timeout, closed pool), every
// method transparently routes to the configured fallback store instead of
// returning an error. If no Fallback was supplied, the fallback is a fresh
// per-process in-memory store — see RedisOptions.Fallback for the fail-open /
// per-instance-divergence implications of that default.
//
// All methods are safe for concurrent use.
type Redis struct {
	client   *goredis.Client
	opts     RedisOptions
	fallback Store
}

// NewRedis creates a new Redis-backed Store.
func NewRedis(opts RedisOptions) *Redis {
	opts.defaults()
	client := goredis.NewClient(&goredis.Options{
		Addr:            opts.Addr,
		Password:        opts.Password,
		DB:              opts.DB,
		PoolSize:        opts.PoolSize,
		MinIdleConns:    opts.MinIdleConns,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
		MaxRetries:      opts.MaxRetries,
		MinRetryBackoff: opts.MinRetryBackoff,
		MaxRetryBackoff: opts.MaxRetryBackoff,
	})
	return &Redis{
		client:   client,
		opts:     opts,
		fallback: opts.Fallback,
	}
}

// NewRedisFromClient creates a Redis store from an existing go-redis client.
// Useful when the caller manages connection pooling externally.
func NewRedisFromClient(client *goredis.Client, opts RedisOptions) *Redis {
	opts.defaults()
	return &Redis{
		client:   client,
		opts:     opts,
		fallback: opts.Fallback,
	}
}

func (r *Redis) prefixed(key string) string {
	return r.opts.KeyPrefix + key
}

// isConnectionError returns true for Redis network/connection errors that
// should trigger the fallback store.
//
// It classifies as a connection error: a closed client pool, any timeout,
// connection-refused / connection-reset / broken-pipe syscall errors, and any
// other net.OpError (e.g. "dial tcp: connect: connection refused", which is NOT
// a timeout and was previously missed) (M-16).
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, goredis.ErrClosed) {
		return true
	}
	// Timeout errors (read/write/dial deadline exceeded).
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// Connection-refused / reset / broken pipe from the OS.
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	// Any other network operation error (non-timeout dial/read/write failure),
	// including ECONNREFUSED wrapped in a *net.OpError without a Timeout() method.
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// Get returns the value for key, or ErrNotFound if absent or expired.
func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, r.prefixed(key)).Result()
	if err == nil {
		return val, nil
	}
	if errors.Is(err, goredis.Nil) {
		return "", ErrNotFound
	}
	if isConnectionError(err) {
		return r.fallback.Get(ctx, key)
	}
	return "", fmt.Errorf("redis get %q: %w", key, err)
}

// Set stores value for key with the given TTL.
func (r *Redis) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := r.client.Set(ctx, r.prefixed(key), value, ttl).Err()
	if err == nil {
		return nil
	}
	if isConnectionError(err) {
		return r.fallback.Set(ctx, key, value, ttl)
	}
	return fmt.Errorf("redis set %q: %w", key, err)
}

// SetNX stores value only if key does not exist. Returns true if the key was set.
func (r *Redis) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	// SET key value NX PX ttl — the modern replacement for the deprecated SETNX.
	// Returns goredis.Nil when NX fails because the key already exists.
	err := r.client.SetArgs(ctx, r.prefixed(key), value, goredis.SetArgs{Mode: "NX", TTL: ttl}).Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if isConnectionError(err) {
		return r.fallback.SetNX(ctx, key, value, ttl)
	}
	return false, fmt.Errorf("redis setnx %q: %w", key, err)
}

// GetSet atomically gets the current value and sets the new one.
//
// This uses the single atomic "SET key value GET [PX ttl]" command (Redis 6.2+)
// rather than a GET+SET TxPipeline. The pipeline approach relied on GET being
// queued before SET and on muddled per-command error handling; SET ... GET
// returns the old value (or nil) in one round trip and applies the TTL
// atomically with the write (M-15).
func (r *Redis) GetSet(ctx context.Context, key, value string, ttl time.Duration) (string, error) {
	args := goredis.SetArgs{Get: true}
	if ttl > 0 {
		args.TTL = ttl
	}
	old, err := r.client.SetArgs(ctx, r.prefixed(key), value, args).Result()
	if err == nil {
		return old, nil
	}
	// SET ... GET returns redis.Nil when the key did not previously exist.
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	if isConnectionError(err) {
		return r.fallback.GetSet(ctx, key, value, ttl)
	}
	return "", fmt.Errorf("redis getset %q: %w", key, err)
}

// incrByScript increments key by delta and sets the TTL only when the INCRBY
// created the key (i.e. the returned value equals delta). This honors the Store
// contract that IncrBy "sets TTL only on creation" — the previous TxPipeline
// issued EXPIRE on every call, giving fixed/sliding windows a sliding TTL that
// never reset (H-5 / STORE-6). Keeping this as one Lua script preserves the
// atomicity of the increment+conditional-expire.
const incrByScript = `
local v = redis.call("INCRBY", KEYS[1], ARGV[1])
local ttl_ms = tonumber(ARGV[2])
if ttl_ms > 0 and v == tonumber(ARGV[1]) then
	redis.call("PEXPIRE", KEYS[1], ttl_ms)
end
return v
`

// IncrBy atomically increments the integer value of key by delta.
// The TTL is applied only when the increment creates the key.
func (r *Redis) IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	pk := r.prefixed(key)
	ttlMs := ttl.Milliseconds()
	res, err := r.client.Eval(ctx, incrByScript, []string{pk}, delta, ttlMs).Result()
	if err != nil {
		if isConnectionError(err) {
			return r.fallback.IncrBy(ctx, key, delta, ttl)
		}
		return 0, fmt.Errorf("redis incrby %q: %w", key, err)
	}
	v, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("redis incrby %q: unexpected result type %T", key, res)
	}
	return v, nil
}

// Eval executes a Lua script atomically on Redis.
// The script parameter is the Lua script body, not a registered name.
func (r *Redis) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	// Prefix keys
	prefixedKeys := make([]string, len(keys))
	for i, k := range keys {
		prefixedKeys[i] = r.prefixed(k)
	}
	val, err := r.client.Eval(ctx, script, prefixedKeys, args...).Result()
	if err == nil {
		return val, nil
	}
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if isConnectionError(err) {
		return r.fallback.Eval(ctx, script, keys, args...)
	}
	return nil, fmt.Errorf("redis eval: %w", err)
}

// Del deletes one or more keys.
func (r *Redis) Del(ctx context.Context, keys ...string) error {
	prefixedKeys := make([]string, len(keys))
	for i, k := range keys {
		prefixedKeys[i] = r.prefixed(k)
	}
	err := r.client.Del(ctx, prefixedKeys...).Err()
	if err == nil {
		return nil
	}
	if isConnectionError(err) {
		return r.fallback.Del(ctx, keys...)
	}
	return fmt.Errorf("redis del: %w", err)
}

// Ping checks Redis connectivity.
func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the Redis client and the fallback store.
func (r *Redis) Close() error {
	err := r.client.Close()
	if ferr := r.fallback.Close(); ferr != nil && err == nil {
		err = ferr
	}
	return err
}

// Client returns the underlying go-redis client.
// Use this to perform operations not covered by the Store interface.
func (r *Redis) Client() *goredis.Client {
	return r.client
}

// ServerTimeMode reports whether this store was configured (via
// RedisOptions.UseServerTime) to use the Redis server clock as the authoritative
// time source for the time-sensitive distributed scripts. Distributed limiters
// query this to decide whether to send use_server_time=1.
//
// The in-memory (*Memory) store also exposes a ServerTimeMode() method (always
// false unless set via NewMemoryWithScripts option), so callers can type-assert
// a store.Store to the interface { ServerTimeMode() bool } uniformly.
func (r *Redis) ServerTimeMode() bool {
	return r.opts.UseServerTime
}

// ServerTimeSkew reads the Redis server clock (via the TIME command) and returns
// the measured skew relative to the local clock, defined as (serverTime -
// localTime). A positive value means the Redis server is ahead of this host; a
// negative value means it is behind.
//
// The measurement includes ~half the round-trip latency as noise: local now is
// sampled immediately before the call and the server timestamp is compared
// against it, so the returned skew is accurate to roughly the RTT/2. For skew
// detection (flagging clocks that are seconds or minutes apart) that resolution
// is more than sufficient.
//
// This is the recommended way to decide whether server-time mode is worth
// enabling for a deployment, and to alert when an app host's clock has drifted.
func (r *Redis) ServerTimeSkew(ctx context.Context) (time.Duration, error) {
	before := time.Now()
	tv, err := r.client.Time(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("redis time: %w", err)
	}
	// Midpoint of the local interval spanning the round trip best estimates the
	// instant the server sampled its clock.
	mid := before.Add(time.Since(before) / 2)
	return tv.Sub(mid), nil
}

// ServerTimeSkewThreshold is the default absolute skew above which
// CheckServerTimeSkew reports the clocks as significantly diverged.
const ServerTimeSkewThreshold = 250 * time.Millisecond

// CheckServerTimeSkew measures the skew (see ServerTimeSkew) and returns the
// skew plus a boolean that is true when |skew| exceeds threshold. If threshold
// is <= 0, ServerTimeSkewThreshold is used. Callers can wire this into a metric
// gauge or emit a warning log when the boolean is set — the library core stays
// dependency-free and does not log on its own.
func (r *Redis) CheckServerTimeSkew(ctx context.Context, threshold time.Duration) (skew time.Duration, exceeded bool, err error) {
	if threshold <= 0 {
		threshold = ServerTimeSkewThreshold
	}
	skew, err = r.ServerTimeSkew(ctx)
	if err != nil {
		return 0, false, err
	}
	abs := skew
	if abs < 0 {
		abs = -abs
	}
	return skew, abs > threshold, nil
}

// ---------------------------------------------------------------------------
// Lua scripts for distributed rate limiting
// ---------------------------------------------------------------------------

// Server-time mode (Redis clock-skew mitigation, ENHANCEMENTS §5.1)
// ------------------------------------------------------------------
// Every time-sensitive Lua script (TokenBucket, GCRA, SlidingWindowLog) accepts
// a trailing `use_server_time` ARGV (default "0"). When it is "1", the script
// overrides the client-supplied `now` (nanoseconds) with a nanosecond timestamp
// derived from Redis's own `TIME` command, making the authoritative clock the
// Redis server rather than the (potentially skewed) calling application. Each of
// those scripts embeds this preamble verbatim (Lua bodies are opaque string
// constants, so they cannot share a Go-level fragment):
//
//	pcall(function() redis.replicate_commands() end)
//	if tostring(use_server_time) == "1" then
//	    local t = redis.call("TIME")
//	    now = (tonumber(t[1]) * 1000000 + tonumber(t[2])) * 1000
//	end
//
// redis.replicate_commands() is called first: TIME is non-deterministic, so a
// script that reads it must switch the connection to *effects* replication
// (replicating the concrete writes) instead of *verbatim* script replication,
// otherwise a replica/AOF re-run would observe a different clock and diverge. On
// Redis >= 5 scripts are effects-replicated by default and the function is a
// no-op returning true; on older servers it performs the switch. It is wrapped
// in pcall so that on the oldest servers (where it may already be forced and
// error) the script still proceeds.
//
// TRADEOFF: server-time mode makes the decision immune to app-fleet clock skew,
// but each call issues an extra in-script `TIME` read and forces effects
// replication (marginally larger replication stream). The default (client time)
// remains unchanged and fully backward compatible.
//
// FixedWindowScript and SlidingWindowCounterScript do NOT take use_server_time:
// they receive only pre-derived window keys / limit / n and never read `now`
// inside the script, so the authoritative clock for those two lives entirely in
// the Go caller (which is a separate, larger change — see distributed callers).

// TokenBucketScript atomically reads tokens + lastRefill, computes refill, checks, and decrements.
// Keys: [bucket_key]
// Args: [capacity, refillRate (tokens/ns), n (tokens to consume), now (unix ns), ttl_ms, use_server_time (0/1)]
// Returns: [allowed (1/0), remaining, refilled]
//
// ttl_ms is the key expiry supplied by the Go caller (time to refill a full
// bucket plus a safety margin). Previously the Go side computed ttlMs and then
// discarded it, and the script derived its own PEXPIRE from capacity/refill_rate
// with no margin (L-1/TB-2); now the caller's ttl_ms is authoritative.
//
// use_server_time (ARGV[6], default "0"): when "1", `now` is overridden with the
// Redis server clock via TIME (see serverTimePreamble) so app clock skew across a
// fleet cannot corrupt the distributed decision.
const TokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local use_server_time = ARGV[6] or "0"
pcall(function() redis.replicate_commands() end)
if tostring(use_server_time) == "1" then
    local t = redis.call("TIME")
    now = (tonumber(t[1]) * 1000000 + tonumber(t[2])) * 1000
end
if ttl_ms == nil or ttl_ms < 1 then
    ttl_ms = math.ceil(capacity / refill_rate / 1000000)
end

local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

-- Refill
local elapsed = now - last_refill
local refilled = elapsed * refill_rate
tokens = math.min(capacity, tokens + refilled)
last_refill = now

if tokens >= n then
    tokens = tokens - n
    redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
    redis.call("PEXPIRE", key, ttl_ms)
    return {1, math.floor(tokens), math.floor(refilled)}
else
    redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
    redis.call("PEXPIRE", key, ttl_ms)
    return {0, math.floor(tokens), math.floor(refilled)}
end
`

// FixedWindowScript atomically checks the window counter and increments it only
// if the request fits. This replaces the "IncrBy(n) then deny if over limit with
// no rollback" logic, where a rejected AllowN(n>limit) permanently poisoned the
// window (count stayed above the limit forever, denying all subsequent requests
// in that window) (H-4/FW-D1).
//
// Keys: [window_key]
// Args: [limit, n, ttl_ms]
// Returns: [allowed (1/0), count]
//
//	On deny, count is the unchanged current count. On allow, count is the new
//	post-increment count. TTL is applied only when the INCRBY creates the key so
//	the window boundary is fixed from its start.
const FixedWindowScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

local current = tonumber(redis.call("GET", key)) or 0
if current + n > limit then
    return {0, current}
end
local new_count = redis.call("INCRBY", key, n)
if new_count == n then
    redis.call("PEXPIRE", key, ttl_ms)
end
return {1, new_count}
`

// GCRAScript atomically reads TAT, computes new TAT, checks, and stores.
// Keys: [tat_key]
// Args: [emission_interval_ns, burst_size, n, now_ns, ttl_ms, use_server_time (0/1)]
// Returns: [allowed (1/0), retry_after_ns]
//
// use_server_time (ARGV[6], default "0"): when "1", `now` is overridden with the
// Redis server clock via TIME (see the server-time preamble docs above).
const GCRAScript = `
local key = KEYS[1]
local emission_interval = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local use_server_time = ARGV[6] or "0"
pcall(function() redis.replicate_commands() end)
if tostring(use_server_time) == "1" then
    local t = redis.call("TIME")
    now = (tonumber(t[1]) * 1000000 + tonumber(t[2])) * 1000
end

local stored_tat = tonumber(redis.call("GET", key)) or now
local tat = math.max(stored_tat, now) + emission_interval * n
local limit_window = now + emission_interval * burst

if tat > limit_window then
    local retry_after = tat - limit_window
    return {0, retry_after}
else
    redis.call("SET", key, tat, "PX", ttl_ms)
    return {1, 0}
end
`

// SlidingWindowCounterScript atomically reads the current+previous fixed-window
// counters, estimates the sliding count, checks it against the limit, and only
// increments the current window if the request fits. This replaces the racy
// check-then-IncrBy in the distributed counter where concurrent callers could
// all pass the check and then all increment, over-admitting past the limit
// (H-3/SWC-D1).
//
// Keys: [current_key, prev_key]
// Args: [limit, n, fraction_millionths, current_ttl_ms]
//   - fraction_millionths = floor(elapsed/window * 1e6): the fraction of the
//     current window already elapsed, scaled to an integer to avoid passing a
//     float through ARGV.
//   - current_ttl_ms is applied to the current-window key only on creation.
//
// Returns: [allowed (1/0), new_current_count, estimated_scaled]
//
//	estimated_scaled = floor(estimated * 1e6) so Go can recover the float.
//
// The estimate uses a plain float comparison (estimated + n <= limit) to match
// the local SlidingWindowCounter, rather than math.Ceil which denied earlier and
// diverged from the in-process limiter (M-2/SWC-D2).
const SlidingWindowCounterScript = `
local current_key = KEYS[1]
local prev_key = KEYS[2]
local limit = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local fraction = tonumber(ARGV[3]) / 1000000.0
local current_ttl_ms = tonumber(ARGV[4])

local current = tonumber(redis.call("GET", current_key)) or 0
local prev = tonumber(redis.call("GET", prev_key)) or 0

local estimated = current + prev * (1.0 - fraction)
local estimated_scaled = math.floor(estimated * 1000000)

if estimated + n > limit then
    return {0, current, estimated_scaled}
end

local new_current = redis.call("INCRBY", current_key, n)
-- Set TTL only when this INCRBY created the current-window key so the window's
-- lifetime is measured from its own start, not extended on every write (M-3).
if new_current == n then
    redis.call("PEXPIRE", current_key, current_ttl_ms)
end
return {1, new_current, estimated_scaled}
`

// SlidingWindowLogScript: prune + ZCARD + (conditionally) ZADD n members + EXPIRE.
// Keys: [log_key]
// Args: [limit, window_ns, now_ns, entry_id, ttl_ms, n, use_server_time (0/1)]
// Returns: [allowed (1/0), count, retry_after_ns]
//
// use_server_time (ARGV[7], default "0"): when "1", `now_ns` is overridden with
// the Redis server clock via TIME (see the server-time preamble docs above). The
// ZSET member scores are then written with the server clock, keeping pruning and
// admission consistent across a skewed fleet.
//
// Matches the local SlidingWindowLog semantics: an AllowN(n) either admits all n
// or none, denies when count+n > limit (not count >= limit, which ignored n and
// over-admitted — H-1/SWL-D1), and on allow adds n DISTINCT members. entry_id is
// used as a per-call prefix and each of the n members is suffixed with "-<i>" so
// two AllowN calls in the same nanosecond cannot collide and silently overwrite
// each other in the ZSET (H-2/SWL-D2). count is the post-admission cardinality so
// remaining is computed consistently.
const SlidingWindowLogScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ns = tonumber(ARGV[2])
local now_ns = tonumber(ARGV[3])
local entry_id = ARGV[4]
local ttl_ms = tonumber(ARGV[5])
local n = tonumber(ARGV[6]) or 1
local use_server_time = ARGV[7] or "0"
pcall(function() redis.replicate_commands() end)
if tostring(use_server_time) == "1" then
    local t = redis.call("TIME")
    now_ns = (tonumber(t[1]) * 1000000 + tonumber(t[2])) * 1000
end

local cutoff = now_ns - window_ns
redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff)
local count = redis.call("ZCARD", key)

if count + n > limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local retry_after = 0
    if oldest[2] then
        retry_after = window_ns - (now_ns - tonumber(oldest[2]))
        if retry_after < 0 then retry_after = 0 end
    end
    redis.call("EXPIRE", key, math.ceil(ttl_ms / 1000))
    return {0, count, retry_after}
else
    for i = 1, n do
        redis.call("ZADD", key, now_ns, entry_id .. "-" .. i)
    end
    redis.call("PEXPIRE", key, ttl_ms)
    return {1, count + n, 0}
end
`
