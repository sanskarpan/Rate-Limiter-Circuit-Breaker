package middleware_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	mw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// benchKeyByHeader keys off a header so we can drive single-key and N-key
// benchmarks without paying for RemoteAddr parsing on the hot path. (okHandler
// is defined in http_test.go, in the same _test package.)
func benchKeyByHeader() mw.Option {
	return mw.WithKeyFunc(mw.KeyByHeader("X-Key"))
}

// BenchmarkHTTP_RateLimit_Allow_SingleKey measures the allow (200) hot path of
// the HTTP middleware for a single key. The bucket is sized huge so every
// request is allowed and we measure steady-state middleware overhead, not denial.
func BenchmarkHTTP_RateLimit_Allow_SingleKey(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	h := mw.RateLimit(tb, benchKeyByHeader())(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Key", "bench-key")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

// BenchmarkHTTP_RateLimit_Allow_100Keys measures the allow hot path under
// key contention across 100 distinct keys.
func BenchmarkHTTP_RateLimit_Allow_100Keys(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	h := mw.RateLimit(tb, benchKeyByHeader())(okHandler)

	reqs := make([]*http.Request, 100)
	for i := range reqs {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Key", fmt.Sprintf("key-%d", i))
		reqs[i] = r
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, reqs[i%100])
	}
}

// BenchmarkGRPC_UnaryInterceptor_Allow measures the allow hot path of the gRPC
// unary interceptor. The limiter is sized huge so every call is admitted.
func BenchmarkGRPC_UnaryInterceptor_Allow(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	interceptor := mw.UnaryServerInterceptor(tb,
		mw.GRPCWithKeyFunc(mw.GRPCKeyByMethod()))

	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/bench.Service/Method"}
	handler := grpc.UnaryHandler(func(_ context.Context, _ any) (any, error) {
		return "ok", nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = interceptor(ctx, nil, info, handler)
	}
}
