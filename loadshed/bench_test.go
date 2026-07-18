package loadshed_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
)

// BenchmarkShedder_Admit measures the admission hot path when the controller is
// not overloaded: every request is admitted and its (near-zero) sojourn is
// recorded via done().
func BenchmarkShedder_Admit(b *testing.B) {
	s := loadshed.New(loadshed.Config{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		accept, done := s.Admit(ctx)
		if accept {
			done()
		}
	}
}

// BenchmarkShedder_AdmitSimple measures the AdmitSimple convenience gate (admit
// + immediate done) hot path.
func BenchmarkShedder_AdmitSimple(b *testing.B) {
	s := loadshed.New(loadshed.Config{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.AdmitSimple(ctx)
	}
}

// BenchmarkShedder_Admit_Prioritized measures the admission path with a priority
// attached to the context (exercises PriorityFromContext on the hot path).
func BenchmarkShedder_Admit_Prioritized(b *testing.B) {
	s := loadshed.New(loadshed.Config{})
	ctx := loadshed.WithPriority(context.Background(), loadshed.PriorityHigh)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		accept, done := s.Admit(ctx)
		if accept {
			done()
		}
	}
}
