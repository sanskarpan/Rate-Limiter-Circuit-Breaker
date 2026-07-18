package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds server configuration.
type Config struct {
	Port              int
	Host              string
	Env               string // "dev" or "prod"
	CORSOrigins       []string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
	// APIKey, when non-empty, requires clients to authenticate mutating/control
	// routes and /metrics with this key. Empty = unauthenticated demo mode.
	APIKey string

	// ── Self-protection / DoS surface (ENHANCEMENTS §7.4) ────────────────────
	// These bound the demo server's own resource usage so the unauthenticated
	// control plane cannot be trivially flooded. They dogfood this library's own
	// rate limiter and are additive with the existing body-size cap (1 MiB),
	// server timeouts and the per-endpoint bulkhead/semaphore caps.

	// MaxRequestBytes is the maximum accepted request body size in bytes.
	// Bodies larger than this are rejected with 413 Request Entity Too Large.
	// Default: 1 MiB (1048576). Must be > 0.
	MaxRequestBytes int64

	// SelfRateLimitPerIP is the sustained per-IP request rate (requests/second)
	// enforced on the mutating control-plane endpoints (allow/execute/configure/
	// force-*/simulate). Requests exceeding the limit receive 429 Too Many
	// Requests. A value <= 0 disables the per-IP self rate limit.
	// Default: 50 req/s.
	SelfRateLimitPerIP float64

	// SelfRateLimitBurst is the per-IP burst capacity (token-bucket size) paired
	// with SelfRateLimitPerIP. It allows short spikes above the sustained rate.
	// A value <= 0 falls back to SelfRateLimitPerIP. Default: 100.
	SelfRateLimitBurst float64

	// MaxInflightControl caps the number of concurrent in-flight requests across
	// the mutating control plane (a global concurrency guard). Requests beyond
	// the cap receive 503 Service Unavailable. A value <= 0 disables the guard.
	// Default: 128.
	MaxInflightControl int
}

// Load reads configuration from environment variables, falling back to defaults.
func Load() (*Config, error) {
	cfg := &Config{
		Port:              8080,
		Host:              "0.0.0.0",
		Env:               "dev",
		CORSOrigins:       []string{"http://localhost:3000"},
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		ShutdownTimeout:   15 * time.Second,

		// Self-protection defaults (§7.4).
		MaxRequestBytes:    1 << 20, // 1 MiB
		SelfRateLimitPerIP: 50,      // 50 req/s sustained per client IP
		SelfRateLimitBurst: 100,     // allow short bursts up to 100
		MaxInflightControl: 128,     // global concurrency cap on control plane
	}

	if v := os.Getenv("PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("PORT must be an integer: %w", err)
		}
		cfg.Port = n
	}
	if v := os.Getenv("HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("ENV"); v != "" {
		cfg.Env = v
	}
	if v := os.Getenv("CORS_ORIGINS"); v != "" {
		parts := strings.Split(v, ",")
		cfg.CORSOrigins = make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				cfg.CORSOrigins = append(cfg.CORSOrigins, trimmed)
			}
		}
	}
	if v := os.Getenv("SERVER_ADDR"); v != "" {
		// Support legacy SERVER_ADDR=host:port format. Parse the host:port pair
		// properly so Addr() does not produce "host:port:8080" (L-14).
		host, portStr, err := net.SplitHostPort(v)
		if err != nil {
			// No port component — treat the whole value as the host.
			cfg.Host = v
		} else {
			cfg.Host = host
			if portStr != "" {
				n, perr := strconv.Atoi(portStr)
				if perr != nil {
					return nil, fmt.Errorf("SERVER_ADDR port must be an integer: %w", perr)
				}
				cfg.Port = n
			}
		}
	}
	if v := os.Getenv("API_KEY"); v != "" {
		cfg.APIKey = strings.TrimSpace(v)
	}
	if v := os.Getenv("MAX_REQUEST_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("MAX_REQUEST_BYTES must be an integer: %w", err)
		}
		cfg.MaxRequestBytes = n
	}
	if v := os.Getenv("SELF_RATE_LIMIT_PER_IP"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("SELF_RATE_LIMIT_PER_IP must be a number: %w", err)
		}
		cfg.SelfRateLimitPerIP = f
	}
	if v := os.Getenv("SELF_RATE_LIMIT_BURST"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("SELF_RATE_LIMIT_BURST must be a number: %w", err)
		}
		cfg.SelfRateLimitBurst = f
	}
	if v := os.Getenv("MAX_INFLIGHT_CONTROL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("MAX_INFLIGHT_CONTROL must be an integer: %w", err)
		}
		cfg.MaxInflightControl = n
	}
	return cfg, nil
}

// Addr returns the host:port string for the server to listen on.
func (c *Config) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// IsDev returns true when running in development mode.
func (c *Config) IsDev() bool { return c.Env == "dev" }
