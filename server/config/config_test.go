package config

import (
	"testing"
	"time"
)

// TestServerAddr_HostPort verifies L-14: SERVER_ADDR=host:port is parsed so
// Addr() returns exactly host:port and not host:port:8080.
func TestServerAddr_HostPort(t *testing.T) {
	t.Setenv("SERVER_ADDR", "1.2.3.4:9000")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Addr(); got != "1.2.3.4:9000" {
		t.Fatalf("expected listen addr 1.2.3.4:9000, got %q", got)
	}
}

// TestServerAddr_HostOnly verifies a bare host still uses the default port.
func TestServerAddr_HostOnly(t *testing.T) {
	t.Setenv("SERVER_ADDR", "127.0.0.1")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Addr(); got != "127.0.0.1:8080" {
		t.Fatalf("expected 127.0.0.1:8080, got %q", got)
	}
}

// TestAPIKey_LoadedFromEnv verifies API_KEY populates the config.
func TestAPIKey_LoadedFromEnv(t *testing.T) {
	t.Setenv("API_KEY", "secret123")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APIKey != "secret123" {
		t.Fatalf("expected APIKey=secret123, got %q", cfg.APIKey)
	}
}

// TestReadHeaderTimeout_Default verifies M-22: a ReadHeaderTimeout default is set.
func TestReadHeaderTimeout_Default(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ReadHeaderTimeout <= 0 {
		t.Fatalf("expected positive ReadHeaderTimeout, got %v", cfg.ReadHeaderTimeout)
	}
}

// TestServerTimeouts_AllConfigured verifies §7.4(2): all four server timeouts
// have sane positive defaults so the demo server is not vulnerable to
// slow-client / connection-hoarding DoS.
func TestServerTimeouts_AllConfigured(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	checks := map[string]time.Duration{
		"ReadHeaderTimeout": cfg.ReadHeaderTimeout,
		"ReadTimeout":       cfg.ReadTimeout,
		"WriteTimeout":      cfg.WriteTimeout,
		"IdleTimeout":       cfg.IdleTimeout,
	}
	for name, d := range checks {
		if d <= 0 {
			t.Fatalf("expected positive %s, got %v", name, d)
		}
	}
}

// TestSelfProtectDefaults verifies §7.4 self-protection defaults are populated.
func TestSelfProtectDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxRequestBytes != 1<<20 {
		t.Fatalf("expected MaxRequestBytes=1MiB, got %d", cfg.MaxRequestBytes)
	}
	if cfg.SelfRateLimitPerIP <= 0 {
		t.Fatalf("expected positive SelfRateLimitPerIP, got %v", cfg.SelfRateLimitPerIP)
	}
	if cfg.SelfRateLimitBurst <= 0 {
		t.Fatalf("expected positive SelfRateLimitBurst, got %v", cfg.SelfRateLimitBurst)
	}
	if cfg.MaxInflightControl <= 0 {
		t.Fatalf("expected positive MaxInflightControl, got %d", cfg.MaxInflightControl)
	}
}

// TestSelfProtectEnvOverrides verifies the §7.4 env vars are parsed.
func TestSelfProtectEnvOverrides(t *testing.T) {
	t.Setenv("MAX_REQUEST_BYTES", "2048")
	t.Setenv("SELF_RATE_LIMIT_PER_IP", "12.5")
	t.Setenv("SELF_RATE_LIMIT_BURST", "40")
	t.Setenv("MAX_INFLIGHT_CONTROL", "7")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxRequestBytes != 2048 {
		t.Fatalf("MaxRequestBytes=%d want 2048", cfg.MaxRequestBytes)
	}
	if cfg.SelfRateLimitPerIP != 12.5 {
		t.Fatalf("SelfRateLimitPerIP=%v want 12.5", cfg.SelfRateLimitPerIP)
	}
	if cfg.SelfRateLimitBurst != 40 {
		t.Fatalf("SelfRateLimitBurst=%v want 40", cfg.SelfRateLimitBurst)
	}
	if cfg.MaxInflightControl != 7 {
		t.Fatalf("MaxInflightControl=%d want 7", cfg.MaxInflightControl)
	}
}

// TestSelfProtectEnvInvalid verifies bad env values are rejected.
func TestSelfProtectEnvInvalid(t *testing.T) {
	t.Setenv("MAX_INFLIGHT_CONTROL", "notanint")
	if _, err := Load(); err == nil {
		t.Fatalf("expected error for invalid MAX_INFLIGHT_CONTROL")
	}
}
