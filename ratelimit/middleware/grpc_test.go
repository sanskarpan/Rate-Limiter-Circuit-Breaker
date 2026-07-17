package middleware_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	mw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mockLimiter for gRPC tests
type grpcMockLimiter struct {
	result ratelimit.Result
}

func (m *grpcMockLimiter) Allow(_ context.Context, _ string) ratelimit.Result { return m.result }
func (m *grpcMockLimiter) AllowN(_ context.Context, _ string, _ int) ratelimit.Result {
	return m.result
}
func (m *grpcMockLimiter) Wait(_ context.Context, _ string) error         { return nil }
func (m *grpcMockLimiter) WaitN(_ context.Context, _ string, _ int) error { return nil }
func (m *grpcMockLimiter) Peek(_ context.Context, _ string) ratelimit.State {
	return ratelimit.State{}
}
func (m *grpcMockLimiter) Reset(_ context.Context, _ string) error { return nil }
func (m *grpcMockLimiter) Close() error                            { return nil }

var noopUnaryHandler grpc.UnaryHandler = func(ctx context.Context, req any) (any, error) {
	return "response", nil
}

var dummyServerInfo = &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

// ---------------------------------------------------------------------------
// UnaryServerInterceptor tests
// ---------------------------------------------------------------------------

func TestGRPC_Unary_AllowsWhenPermitted(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}
	interceptor := mw.UnaryServerInterceptor(limiter)

	ctx := context.Background()
	resp, err := interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "response" {
		t.Fatalf("expected 'response', got %v", resp)
	}
}

func TestGRPC_Unary_RejectsWhenRateLimited(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: false, Limit: 10, Remaining: 0}}
	interceptor := mw.UnaryServerInterceptor(limiter)

	ctx := context.Background()
	_, err := interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", st.Code())
	}
}

func TestGRPC_Unary_SkipMethod(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: false}} // would deny
	interceptor := mw.UnaryServerInterceptor(limiter,
		mw.GRPCWithSkipMethods("/test.Service/Method"),
	)

	ctx := context.Background()
	resp, err := interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler)
	if err != nil {
		t.Fatalf("expected method to be skipped, got error: %v", err)
	}
	if resp != "response" {
		t.Fatalf("expected 'response', got %v", resp)
	}
}

func TestGRPC_Unary_KeyExtraction(t *testing.T) {
	var extractedKey string
	limiter := &captureKeyLimiter{onAllow: func(key string) {
		extractedKey = key
	}}
	interceptor := mw.UnaryServerInterceptor(limiter,
		mw.GRPCWithKeyFunc(mw.GRPCKeyByMetadata("x-user-id")),
	)

	md := metadata.Pairs("x-user-id", "user:123")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler) //nolint:errcheck

	if extractedKey != "user:123" {
		t.Fatalf("expected key 'user:123', got %q", extractedKey)
	}
}

func TestGRPC_Unary_KeyByMethod(t *testing.T) {
	var extractedKey string
	limiter := &captureKeyLimiter{onAllow: func(key string) {
		extractedKey = key
	}}
	interceptor := mw.UnaryServerInterceptor(limiter,
		mw.GRPCWithKeyFunc(mw.GRPCKeyByMethod()),
	)

	ctx := context.Background()
	interceptor(ctx, nil, dummyServerInfo, noopUnaryHandler) //nolint:errcheck

	if extractedKey != "/test.Service/Method" {
		t.Fatalf("expected method as key, got %q", extractedKey)
	}
}

// ---------------------------------------------------------------------------
// StreamServerInterceptor tests
// ---------------------------------------------------------------------------

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

var noopStreamHandler grpc.StreamHandler = func(srv any, stream grpc.ServerStream) error {
	return nil
}

var dummyStreamInfo = &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}

func TestGRPC_Stream_AllowsWhenPermitted(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: true}}
	interceptor := mw.StreamServerInterceptor(limiter)

	ss := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, dummyStreamInfo, noopStreamHandler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGRPC_Stream_RejectsWhenRateLimited(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: false}}
	interceptor := mw.StreamServerInterceptor(limiter)

	ss := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, dummyStreamInfo, noopStreamHandler)
	if err == nil {
		t.Fatal("expected error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", st.Code())
	}
}

func TestGRPC_Stream_SkipMethod(t *testing.T) {
	limiter := &grpcMockLimiter{result: ratelimit.Result{Allowed: false}}
	interceptor := mw.StreamServerInterceptor(limiter,
		mw.GRPCWithSkipMethods("/test.Service/Stream"),
	)

	ss := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, dummyStreamInfo, noopStreamHandler)
	if err != nil {
		t.Fatalf("expected method to be skipped, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Key extractor helpers
// ---------------------------------------------------------------------------

type captureKeyLimiter struct {
	onAllow func(key string)
}

func (c *captureKeyLimiter) Allow(_ context.Context, key string) ratelimit.Result {
	if c.onAllow != nil {
		c.onAllow(key)
	}
	return ratelimit.Result{Allowed: true}
}
func (c *captureKeyLimiter) AllowN(_ context.Context, key string, _ int) ratelimit.Result {
	return c.Allow(context.Background(), key)
}
func (c *captureKeyLimiter) Wait(_ context.Context, _ string) error         { return nil }
func (c *captureKeyLimiter) WaitN(_ context.Context, _ string, _ int) error { return nil }
func (c *captureKeyLimiter) Peek(_ context.Context, _ string) ratelimit.State {
	return ratelimit.State{}
}
func (c *captureKeyLimiter) Reset(_ context.Context, _ string) error { return nil }
func (c *captureKeyLimiter) Close() error                            { return nil }

// TestGRPC_KeyByMetadata_FallbackEmpty verifies empty string when metadata absent.
func TestGRPC_KeyByMetadata_FallbackEmpty(t *testing.T) {
	kf := mw.GRPCKeyByMetadata("x-user-id")
	key := kf(context.Background(), "/test.Service/Method")
	if key != "" {
		t.Fatalf("expected empty key without metadata, got %q", key)
	}
}

// TestGRPC_ErrUnused is just here to avoid "errors imported and not used" lint.
var _ = errors.New
