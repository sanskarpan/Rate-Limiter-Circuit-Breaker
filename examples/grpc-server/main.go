// Package main demonstrates gRPC server interceptors for rate limiting and circuit breaking.
package main

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	cbmw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker/middleware"
	ratelimitmw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// echoServer is a minimal gRPC server for demonstration purposes.
// In real code, implement your proto-generated service interface here.
type echoServer struct{}

func main() {
	// 1. Create rate limiter: 100 req/s per key, burst 20.
	limiter := tokenbucket.New(100, 20)
	defer limiter.Close()

	// 2. Create circuit breaker.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "grpc-backend",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       20,
		FailureThreshold: 5,
		OpenTimeout:      30 * time.Second,
	})

	// 3. Build gRPC server with chained interceptors.
	//    Order: rate limit → circuit breaker → handler
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			// Rate limit by metadata header "x-user-id"; fall back to "" (global)
			ratelimitmw.UnaryServerInterceptor(
				limiter,
				ratelimitmw.GRPCWithKeyFunc(ratelimitmw.GRPCKeyByMetadata("x-user-id")),
				ratelimitmw.GRPCWithSkipMethods("/grpc.health.v1.Health/Check"),
			),
			// Circuit breaker wraps the handler
			cbmw.CBUnaryServerInterceptor(cb),
		),
		grpc.ChainStreamInterceptor(
			ratelimitmw.StreamServerInterceptor(limiter),
			cbmw.CBStreamServerInterceptor(cb),
		),
	)

	// Register your services here:
	// pb.RegisterMyServiceServer(srv, &myServiceImpl{})

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	log.Println("gRPC server running on :9090")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// exampleUnaryHandler shows how to handle circuit-open responses in gRPC clients.
func exampleUnaryHandler(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			// Circuit is open — return a fallback or propagate upstream
			return nil, status.Error(codes.Unavailable, "upstream circuit open")
		}
		return nil, err
	}
	return resp, nil
}

// Ensure echoServer satisfies the expected interface at compile time.
var _ = &echoServer{}
