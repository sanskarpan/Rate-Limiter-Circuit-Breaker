package loadshed_test

import (
	"context"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
)

// ExampleShedder_Admit shows the call-site convention: Admit at the admission
// point, defer done() to feed the guarded section's duration back into the
// CoDel sojourn-time control loop.
func ExampleShedder_Admit() {
	s := loadshed.New(loadshed.Config{
		Target:   loadshed.DefaultTarget,
		Interval: loadshed.DefaultInterval,
	})

	// Attach a priority so the shedder protects important work under overload.
	ctx := loadshed.WithPriority(context.Background(), loadshed.PriorityHigh)

	accept, done := s.Admit(ctx)
	if !accept {
		fmt.Println("shed")
		return
	}
	defer done() // records the sojourn/latency of the guarded section

	fmt.Println("admitted")
	// Output: admitted
}
