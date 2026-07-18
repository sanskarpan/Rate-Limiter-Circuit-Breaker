package connectmw_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/connectmw"
)

func newCBTestBreaker(name string) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New(circuitbreaker.Config{
		Name:             name,
		FailureThreshold: 2,
		MinimumRequests:  1,
		OpenTimeout:      time.Minute,
	})
}

// newCBHandler serves Echo, returning CodeInternal when the request text is
// "fail". calls counts how many times the underlying handler actually ran.
func newCBHandler(interceptor connect.Interceptor, calls *int) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(procedure, connect.NewUnaryHandler(
		procedure,
		func(_ context.Context, req *connect.Request[msg]) (*connect.Response[msg], error) {
			*calls++
			if req.Msg.Text == "fail" {
				return nil, connect.NewError(connect.CodeInternal, errors.New("boom"))
			}
			return connect.NewResponse(&msg{Text: req.Msg.Text}), nil
		},
		connect.WithCodec(jsonCodec{}),
		connect.WithInterceptors(interceptor),
	))
	return mux
}

func TestCircuitBreaker_OpensAndShortCircuits(t *testing.T) {
	cb := newCBTestBreaker("connect-cb")
	var calls int

	srv := httptest.NewServer(newCBHandler(connectmw.CircuitBreaker(cb), &calls))
	defer srv.Close()
	client := connect.NewClient[msg, msg](srv.Client(), srv.URL+procedure, connect.WithCodec(jsonCodec{}))

	// Two server-fault (CodeInternal) responses trip the breaker.
	for i := 0; i < 2; i++ {
		_, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "fail"}))
		if got := connect.CodeOf(err); got != connect.CodeInternal {
			t.Fatalf("request %d: code = %v, want Internal", i, got)
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open", cb.State())
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}

	// Next call short-circuits with CodeUnavailable; handler is NOT invoked.
	_, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "ok"}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("open-circuit code = %v, want Unavailable", got)
	}
	if calls != 2 {
		t.Fatalf("handler ran while circuit open: calls = %d, want 2", calls)
	}
}

func TestCircuitBreaker_ClientErrorsDoNotTrip(t *testing.T) {
	// A client-fault code (InvalidArgument) must not count as a breaker failure.
	cb := newCBTestBreaker("connect-cb-client")
	var calls int

	mux := http.NewServeMux()
	mux.Handle(procedure, connect.NewUnaryHandler(
		procedure,
		func(_ context.Context, _ *connect.Request[msg]) (*connect.Response[msg], error) {
			calls++
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("bad input"))
		},
		connect.WithCodec(jsonCodec{}),
		connect.WithInterceptors(connectmw.CircuitBreaker(cb)),
	))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := connect.NewClient[msg, msg](srv.Client(), srv.URL+procedure, connect.WithCodec(jsonCodec{}))

	for i := 0; i < 5; i++ {
		_, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "x"}))
		if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
			t.Fatalf("request %d: code = %v, want InvalidArgument", i, got)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed (client errors must not trip)", cb.State())
	}
	if calls != 5 {
		t.Fatalf("handler calls = %d, want 5 (breaker never opened)", calls)
	}
}

func TestCircuitBreaker_SuccessPassesThrough(t *testing.T) {
	cb := newCBTestBreaker("connect-cb-ok")
	var calls int

	srv := httptest.NewServer(newCBHandler(connectmw.CircuitBreaker(cb), &calls))
	defer srv.Close()
	client := connect.NewClient[msg, msg](srv.Client(), srv.URL+procedure, connect.WithCodec(jsonCodec{}))

	for i := 0; i < 5; i++ {
		resp, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "hi"}))
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if resp.Msg.Text != "hi" {
			t.Fatalf("request %d: echo = %q, want hi", i, resp.Msg.Text)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed", cb.State())
	}
}

func ExampleCircuitBreaker() {
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "connect"})

	interceptor := connectmw.CircuitBreaker(cb)
	_ = interceptor

	fmt.Println("mounted")
	// Output: mounted
}
