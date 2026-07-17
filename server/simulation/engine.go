// Package simulation provides a load simulation engine for testing rate limiters
// and circuit breakers under various traffic patterns.
package simulation

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// Pattern describes the traffic pattern for the simulation.
type Pattern string

const (
	// PatternConstant sends requests at a constant rate.
	PatternConstant Pattern = "constant"
	// PatternBurst sends a burst of requests then idle.
	PatternBurst Pattern = "burst"
	// PatternRamp gradually increases the request rate.
	PatternRamp Pattern = "ramp"
	// PatternRandom sends requests at a random rate.
	PatternRandom Pattern = "random"
)

// Config configures a simulation run.
type Config struct {
	// Pattern is the traffic shape.
	Pattern Pattern `json:"pattern"`
	// Duration is how long to run the simulation.
	Duration time.Duration `json:"duration_ms"`
	// RequestsPerSecond is the target RPS (pattern dependent).
	RequestsPerSecond float64 `json:"requests_per_second"`
	// Concurrency is the number of concurrent workers.
	Concurrency int `json:"concurrency"`
	// Key is the rate limit key to use.
	Key string `json:"key"`
}

// Result holds aggregated results from a simulation run.
type Result struct {
	TotalRequests int64   `json:"total_requests"`
	Allowed       int64   `json:"allowed"`
	Denied        int64   `json:"denied"`
	AllowedRate   float64 `json:"allowed_rate"`
	DeniedRate    float64 `json:"denied_rate"`
	DurationMs    int64   `json:"duration_ms"`
}

// Engine runs load simulations against a rate limiter.
type Engine struct {
	logger *slog.Logger
}

// New creates a new simulation Engine.
func New(logger *slog.Logger) *Engine {
	return &Engine{logger: logger}
}

// Run executes a simulation against the given limiter and returns results.
func (e *Engine) Run(ctx context.Context, limiter ratelimit.Limiter, cfg Config) (*Result, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Key == "" {
		cfg.Key = "simulation"
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 5 * time.Second
	}
	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = 10
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var (
		totalRequests atomic.Int64
		allowed       atomic.Int64
		denied        atomic.Int64
	)

	start := time.Now()
	interval := time.Duration(float64(time.Second) / cfg.RequestsPerSecond)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			e.runWorker(ctx, limiter, cfg, workerID, interval, &totalRequests, &allowed, &denied)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := totalRequests.Load()
	a := allowed.Load()
	d := denied.Load()

	result := &Result{
		TotalRequests: total,
		Allowed:       a,
		Denied:        d,
		DurationMs:    elapsed.Milliseconds(),
	}
	if total > 0 {
		result.AllowedRate = float64(a) / float64(total)
		result.DeniedRate = float64(d) / float64(total)
	}

	e.logger.Info("simulation completed",
		"pattern", cfg.Pattern,
		"total", total,
		"allowed", a,
		"denied", d,
		"duration_ms", elapsed.Milliseconds(),
	)

	return result, nil
}

func (e *Engine) runWorker(
	ctx context.Context,
	limiter ratelimit.Limiter,
	cfg Config,
	workerID int,
	interval time.Duration,
	total, allowed, denied *atomic.Int64,
) {
	ticker := time.NewTicker(e.adjustInterval(cfg.Pattern, interval, workerID))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			key := fmt.Sprintf("%s:worker:%d", cfg.Key, workerID)
			result := limiter.Allow(ctx, key)
			total.Add(1)
			if result.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}

			// Adjust ticker for dynamic patterns
			if cfg.Pattern == PatternRamp || cfg.Pattern == PatternRandom {
				ticker.Reset(e.adjustInterval(cfg.Pattern, interval, workerID))
			}
		}
	}
}

func (e *Engine) adjustInterval(pattern Pattern, base time.Duration, workerID int) time.Duration {
	switch pattern {
	case PatternRandom:
		// Jitter ±50%
		jitter := float64(base) * (0.5 + rand.Float64())
		return time.Duration(jitter)
	case PatternBurst:
		// Workers 0 sends fast, others send slow
		if workerID == 0 {
			return base / 10
		}
		return base * 5
	case PatternRamp:
		// Gradually decrease interval (increase rate)
		factor := 1.0 - float64(workerID)*0.1
		if factor < 0.1 {
			factor = 0.1
		}
		return time.Duration(float64(base) * factor)
	default:
		return base
	}
}
