package connectmw_test

import (
	"fmt"

	"connectrpc.com/connect"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/connectmw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func ExampleRateLimit() {
	lim := tokenbucket.New(100, 10)
	defer lim.Close()

	// Rate limit unary RPCs by API key, falling back to peer address.
	interceptor := connectmw.RateLimit(lim,
		connectmw.WithKeyFunc(connectmw.KeyByHeader("X-API-Key")),
	)

	// Pass it to any connect handler via connect.WithInterceptors(interceptor).
	_ = connect.WithInterceptors(interceptor)
	fmt.Println("interceptor ready")
	// Output: interceptor ready
}
