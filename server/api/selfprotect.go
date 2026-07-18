package api

import (
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// SelfProtectConfig configures the demo server's self-protection layer
// (ENHANCEMENTS §7.4). Zero/negative values disable the corresponding guard,
// so the middleware is always safe to install. The defaults are supplied by
// server/config.
//
// The self-protection layer dogfoods THIS library's own rate limiter: a
// per-client-IP token bucket rejects floods with 429, while a global counting
// semaphore caps concurrent in-flight control-plane requests with 503. Both are
// additive with the existing 1 MiB body cap, server timeouts, the CB-execute
// bulkhead and the /simulate semaphore.
type SelfProtectConfig struct {
	// MaxRequestBytes is the maximum accepted request body size in bytes.
	// <= 0 falls back to the package default (1 MiB).
	MaxRequestBytes int64
	// RatePerIP is the sustained per-IP request rate (req/s). <= 0 disables the
	// per-IP rate limit.
	RatePerIP float64
	// Burst is the per-IP token-bucket capacity. <= 0 falls back to RatePerIP.
	Burst float64
	// MaxInflight caps concurrent in-flight control-plane requests. <= 0
	// disables the global concurrency guard.
	MaxInflight int
}

// effectiveMaxRequestBytes returns the body-size cap to enforce, falling back to
// the package default when unset.
func (c SelfProtectConfig) effectiveMaxRequestBytes() int64 {
	if c.MaxRequestBytes <= 0 {
		return maxRequestBodyBytes
	}
	return c.MaxRequestBytes
}

// DefaultSelfProtectConfig returns the built-in safe defaults used when a caller
// constructs the router without explicit self-protection settings. They mirror
// the defaults in server/config.
func DefaultSelfProtectConfig() SelfProtectConfig {
	return SelfProtectConfig{
		MaxRequestBytes: maxRequestBodyBytes,
		RatePerIP:       50,
		Burst:           100,
		MaxInflight:     128,
	}
}

// selfProtector holds the runtime state for the self-protection middleware: the
// dogfooded per-IP limiter and the global in-flight concurrency guard.
type selfProtector struct {
	limiter  ratelimit.Limiter // nil when per-IP limiting is disabled
	inflight chan struct{}     // nil when the concurrency guard is disabled
}

// newSelfProtector builds a selfProtector from cfg. The returned closer must be
// called on shutdown to release the limiter's resources; it is nil-safe.
func newSelfProtector(cfg SelfProtectConfig) (*selfProtector, func()) {
	sp := &selfProtector{}
	var closer func()

	if cfg.RatePerIP > 0 {
		burst := cfg.Burst
		if burst <= 0 {
			burst = cfg.RatePerIP
		}
		// Dogfood the library's token bucket: capacity=burst, refill=RatePerIP/s.
		lim := tokenbucket.New(burst, cfg.RatePerIP)
		sp.limiter = lim
		closer = func() { _ = lim.Close() }
	}
	if cfg.MaxInflight > 0 {
		sp.inflight = make(chan struct{}, cfg.MaxInflight)
	}
	if closer == nil {
		closer = func() {}
	}
	return sp, closer
}

// Middleware returns an http.Handler wrapper enforcing the per-IP rate limit and
// the global concurrency cap. It is intended to wrap ONLY the mutating
// control-plane endpoints so that read-only/health traffic is never throttled by
// the server's self-protection (normal traffic is unaffected).
func (sp *selfProtector) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Global concurrency guard first: shed load with 503 before doing any
		// per-key work when the server is already saturated.
		if sp.inflight != nil {
			select {
			case sp.inflight <- struct{}{}:
				defer func() { <-sp.inflight }()
			default:
				writeError(w, r, http.StatusServiceUnavailable, "server_busy",
					"server is at capacity; retry later")
				return
			}
		}

		// Per-IP rate limit (dogfooded token bucket).
		if sp.limiter != nil {
			ip := clientIP(r)
			res := sp.limiter.Allow(r.Context(), ip)
			if !res.Allowed {
				if ra := res.RetryAfter.Milliseconds(); ra > 0 {
					w.Header().Set("Retry-After", retryAfterSeconds(ra))
				}
				writeError(w, r, http.StatusTooManyRequests, "rate_limited",
					"per-IP request rate exceeded; retry later")
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// clientIP extracts a best-effort client identifier for per-IP rate limiting.
// It prefers the leftmost X-Forwarded-For entry (demo convenience) and falls
// back to the connection's remote host. The value is only used as a rate-limit
// key, never for auth decisions.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			xff = xff[:i]
		}
		if h := strings.TrimSpace(xff); h != "" {
			return h
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		if r.RemoteAddr != "" {
			return r.RemoteAddr
		}
		return "unknown"
	}
	return host
}

// retryAfterSeconds converts milliseconds to a whole-second Retry-After value,
// rounding up and clamping to at least 1.
func retryAfterSeconds(ms int64) string {
	secs := (ms + 999) / 1000
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}
