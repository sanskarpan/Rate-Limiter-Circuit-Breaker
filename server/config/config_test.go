package config

import "testing"

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
