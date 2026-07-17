package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/resilience/circuitbreaker"
	cbmw "github.com/sanskarpan/resilience/circuitbreaker/middleware"
	"github.com/sanskarpan/resilience/internal/clock"
)

func newTestCB(threshold int) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New(circuitbreaker.Config{
		Name:             "test",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: threshold,
		OpenTimeout:      10 * time.Second,
		Clock:            clock.RealClock{},
	})
}

var dummyUnaryInfo = &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

// ---------------------------------------------------------------------------
// Unary interceptor
// ---------------------------------------------------------------------------

func TestCBGRPC_Unary_SuccessPassesThrough(t *testing.T) {
	cb := newTestCB(3)
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	resp, err := interceptor(context.Background(), nil, dummyUnaryInfo,
		func(ctx context.Context, req any) (any, error) {
			return "ok", nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}
}

func TestCBGRPC_Unary_InternalErrorTripsBreaker(t *testing.T) {
	cb := newTestCB(2)
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	internalErr := status.Error(codes.Internal, "backend error")

	for i := 0; i < 2; i++ {
		interceptor(context.Background(), nil, dummyUnaryInfo, //nolint:errcheck
			func(ctx context.Context, req any) (any, error) {
				return nil, internalErr
			})
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen after %d Internal errors, got %s", 2, cb.State())
	}
}

func TestCBGRPC_Unary_OpenCircuitReturnsUnavailable(t *testing.T) {
	cb := newTestCB(1)
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	// Trip the circuit
	interceptor(context.Background(), nil, dummyUnaryInfo, //nolint:errcheck
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.Internal, "error")
		})

	// Next call should get Unavailable
	_, err := interceptor(context.Background(), nil, dummyUnaryInfo,
		func(ctx context.Context, req any) (any, error) {
			return "should not reach", nil
		})

	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", st.Code())
	}
}

func TestCBGRPC_Unary_NotFoundDoesNotTripBreaker(t *testing.T) {
	cb := newTestCB(1)
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	// NotFound is a client error — should NOT trip the breaker
	_, err := interceptor(context.Background(), nil, dummyUnaryInfo,
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.NotFound, "not found")
		})

	if !errors.Is(err, status.Error(codes.NotFound, "not found")) {
		// error should pass through
		st, _ := status.FromError(err)
		if st.Code() != codes.NotFound {
			t.Fatalf("expected NotFound error to pass through, got %v", err)
		}
	}

	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected CB to remain Closed on client error, got %s", cb.State())
	}
}

func TestCBGRPC_Unary_NonGRPCErrorTripsBreaker(t *testing.T) {
	cb := newTestCB(1)
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	// Non-gRPC error should trip the breaker
	interceptor(context.Background(), nil, dummyUnaryInfo, //nolint:errcheck
		func(ctx context.Context, req any) (any, error) {
			return nil, errors.New("raw error")
		})

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen after non-gRPC error, got %s", cb.State())
	}
}

// TestCBGRPC_Unary_DeadlineExceededDoesNotTripBreaker verifies M-7: a
// DeadlineExceeded response is treated as caller cancellation and must NOT count
// as a circuit-breaker failure.
func TestCBGRPC_Unary_DeadlineExceededDoesNotTripBreaker(t *testing.T) {
	cb := newTestCB(1) // a single failure would trip it
	interceptor := cbmw.CBUnaryServerInterceptor(cb)

	deadlineErr := status.Error(codes.DeadlineExceeded, "deadline exceeded")

	_, err := interceptor(context.Background(), nil, dummyUnaryInfo,
		func(ctx context.Context, req any) (any, error) {
			return nil, deadlineErr
		})

	// The error should pass through as DeadlineExceeded (not swallowed).
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded to pass through, got %v", err)
	}

	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("DeadlineExceeded must not trip the breaker, got %s", cb.State())
	}
}

// ---------------------------------------------------------------------------
// Stream interceptor
// ---------------------------------------------------------------------------

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

var dummyStreamInfo = &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}

func TestCBGRPC_Stream_SuccessPassesThrough(t *testing.T) {
	cb := newTestCB(3)
	interceptor := cbmw.CBStreamServerInterceptor(cb)

	ss := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, ss, dummyStreamInfo, func(srv any, stream grpc.ServerStream) error {
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCBGRPC_Stream_OpenCircuitReturnsUnavailable(t *testing.T) {
	cb := newTestCB(1)
	interceptor := cbmw.CBStreamServerInterceptor(cb)

	ss := &mockServerStream{ctx: context.Background()}

	// Trip the circuit
	interceptor(nil, ss, dummyStreamInfo, func(srv any, stream grpc.ServerStream) error { //nolint:errcheck
		return status.Error(codes.Internal, "error")
	})

	// Next call should fail
	err := interceptor(nil, ss, dummyStreamInfo, func(srv any, stream grpc.ServerStream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}
