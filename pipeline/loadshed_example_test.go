package pipeline_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
)

// ExampleBuilder_LoadShed shows wiring a CoDel load-shedding stage as the
// outermost admission control. Attach a priority to the request context so the
// shedder protects high-priority work under overload.
func ExampleBuilder_LoadShed() {
	shedder := loadshed.New(loadshed.Config{
		Target:   loadshed.DefaultTarget,
		Interval: loadshed.DefaultInterval,
	})

	p := pipeline.New().
		LoadShed(shedder). // admission control, sorts outermost
		Build()

	ctx := loadshed.WithPriority(context.Background(), loadshed.PriorityHigh)
	err := p.Execute(ctx, func(ctx context.Context) error {
		return nil
	})

	switch {
	case errors.Is(err, pipeline.ErrLoadShed):
		fmt.Println("shed under overload")
	case err != nil:
		fmt.Println("error:", err)
	default:
		fmt.Println("handled")
	}
	// Output: handled
}
