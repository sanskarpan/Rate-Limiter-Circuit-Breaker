package middleware_test

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	mw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// grpcSpyLimiter records the last n passed to AllowN and delegates to an inner
// limiter for realistic allow/deny behavior.
type grpcSpyLimiter struct {
	inner ratelimit.Limiter

	mu       sync.Mutex
	lastN    int
	allowHit int
	allowNHt int
}

func (s *grpcSpyLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	s.mu.Lock()
	s.allowHit++
	s.lastN = 1
	s.mu.Unlock()
	return s.inner.Allow(ctx, key)
}

func (s *grpcSpyLimiter) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	s.mu.Lock()
	s.allowNHt++
	s.lastN = n
	s.mu.Unlock()
	return s.inner.AllowN(ctx, key, n)
}

func (s *grpcSpyLimiter) Wait(ctx context.Context, key string) error {
	return s.inner.Wait(ctx, key)
}
func (s *grpcSpyLimiter) WaitN(ctx context.Context, key string, n int) error {
	return s.inner.WaitN(ctx, key, n)
}
func (s *grpcSpyLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	return s.inner.Peek(ctx, key)
}
func (s *grpcSpyLimiter) Reset(ctx context.Context, key string) error {
	return s.inner.Reset(ctx, key)
}
func (s *grpcSpyLimiter) Close() error { return s.inner.Close() }

// captureStream records headers set via grpc.SetHeader so the test can assert
// on the x-ratelimit-cost metadata.
type captureStream struct {
	md metadata.MD
}

func (c *captureStream) Method() string                  { return "/test.Service/Method" }
func (c *captureStream) SetHeader(md metadata.MD) error  { c.md = metadata.Join(c.md, md); return nil }
func (c *captureStream) SendHeader(md metadata.MD) error { return nil }
func (c *captureStream) SetTrailer(md metadata.MD) error { return nil }

// TestGRPC_Unary_WithCost verifies GRPCWithCost consumes n tokens via AllowN,
// exhausts the limiter n times faster, and sets the x-ratelimit-cost header.
func TestGRPC_Unary_WithCost(t *testing.T) {
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	spy := &grpcSpyLimiter{inner: tb}

	interceptor := mw.UnaryServerInterceptor(spy,
		mw.GRPCWithKeyFunc(func(context.Context, string) string { return "k" }),
		mw.GRPCWithCost(func(context.Context, string) int { return 5 }),
	)

	call := func() error {
		stream := &captureStream{}
		ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
		_, err := interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler)
		if err == nil {
			if got := stream.md.Get("x-ratelimit-cost"); len(got) == 0 || got[0] != "5" {
				t.Fatalf("expected x-ratelimit-cost=5 header, got %v", got)
			}
		}
		return err
	}

	// Two calls of cost 5 fit in capacity 10; the third is exhausted.
	if err := call(); err != nil {
		t.Fatalf("call1 should be allowed: %v", err)
	}
	if err := call(); err != nil {
		t.Fatalf("call2 should be allowed: %v", err)
	}
	err := call()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("call3 should be ResourceExhausted (5x-faster exhaustion), got %v", err)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.lastN != 5 {
		t.Fatalf("expected AllowN called with n=5, last n=%d", spy.lastN)
	}
	if spy.allowHit != 0 {
		t.Fatalf("expected single-token Allow never used, hits=%d", spy.allowHit)
	}
}

// TestGRPC_Unary_WithCost_DefaultsToOne verifies no cost func keeps the
// single-token Allow path and a cost header of 1.
func TestGRPC_Unary_WithCost_DefaultsToOne(t *testing.T) {
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	spy := &grpcSpyLimiter{inner: tb}

	interceptor := mw.UnaryServerInterceptor(spy,
		mw.GRPCWithKeyFunc(func(context.Context, string) string { return "k" }),
	)
	stream := &captureStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	if _, err := interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler); err != nil {
		t.Fatalf("call should be allowed: %v", err)
	}
	if got := stream.md.Get("x-ratelimit-cost"); len(got) == 0 || got[0] != "1" {
		t.Fatalf("expected default x-ratelimit-cost=1, got %v", got)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.allowHit != 1 || spy.allowNHt != 0 {
		t.Fatalf("expected single-token Allow path, allowHit=%d allowNHt=%d", spy.allowHit, spy.allowNHt)
	}
}
