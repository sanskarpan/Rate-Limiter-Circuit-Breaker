package connectmw_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/connectmw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// msg is a trivial request/response payload carried by the jsonCodec below, so
// the test needs no protobuf toolchain.
type msg struct {
	Text string `json:"text"`
}

// jsonCodec is a minimal connect.Codec so we can run an end-to-end unary RPC
// over plain JSON. Its name matches on both the handler and the client.
type jsonCodec struct{}

func (jsonCodec) Name() string                  { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

const procedure = "/test.Service/Echo"

func newHandler(interceptor connect.Interceptor) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(procedure, connect.NewUnaryHandler(
		procedure,
		func(_ context.Context, req *connect.Request[msg]) (*connect.Response[msg], error) {
			return connect.NewResponse(&msg{Text: req.Msg.Text}), nil
		},
		connect.WithCodec(jsonCodec{}),
		connect.WithInterceptors(interceptor),
	))
	return mux
}

func TestRateLimit_OKThenResourceExhausted(t *testing.T) {
	lim := tokenbucket.New(2, 0.001)
	defer lim.Close()

	interceptor := connectmw.RateLimit(lim,
		connectmw.WithKeyFunc(func(_ context.Context, _ connect.AnyRequest) string { return "k" }),
	)

	srv := httptest.NewServer(newHandler(interceptor))
	defer srv.Close()

	client := connect.NewClient[msg, msg](
		srv.Client(), srv.URL+procedure, connect.WithCodec(jsonCodec{}),
	)

	// First two requests succeed (bucket capacity 2).
	for i := 0; i < 2; i++ {
		resp, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "hi"}))
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if resp.Msg.Text != "hi" {
			t.Fatalf("request %d: echo = %q, want hi", i, resp.Msg.Text)
		}
	}

	// Third request is denied with CodeResourceExhausted.
	_, err := client.CallUnary(context.Background(), connect.NewRequest(&msg{Text: "hi"}))
	if err == nil {
		t.Fatal("third request: want error, got nil")
	}
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("third request: code = %v, want ResourceExhausted", got)
	}
}
