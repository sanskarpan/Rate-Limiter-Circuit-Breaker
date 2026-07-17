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
}

// Load reads configuration from environment variables, falling back to defaults.
func Load() (*Config, error) {
	cfg := &Config{
		Port:            8080,
		Host:            "0.0.0.0",
		Env:             "dev",
		CORSOrigins:     []string{"http://localhost:3000"},
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		ShutdownTimeout:   15 * time.Second,
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
	return cfg, nil
}

// Addr returns the host:port string for the server to listen on.
func (c *Config) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// IsDev returns true when running in development mode.
func (c *Config) IsDev() bool { return c.Env == "dev" }
