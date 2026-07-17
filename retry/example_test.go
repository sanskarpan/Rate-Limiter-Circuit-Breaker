package retry_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/sanskarpan/resilience/retry"
	"github.com/sanskarpan/resilience/retry/backoff"
)

// ExamplePolicy_Do_constant demonstrates retrying with a constant backoff.
func ExamplePolicy_Do_constant() {
	attempts := 0
	p := &retry.Policy{
		MaxAttempts: 3,
		Backoff:     backoff.Constant(0), // zero delay for example
	}

	err := p.Do(context.Background(), func(_ context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})

	fmt.Printf("err=%v attempts=%d\n", err, attempts)
	// Output:
	// err=<nil> attempts=3
}

// ExamplePolicy_Do_exponential demonstrates exponential backoff configuration.
func ExamplePolicy_Do_exponential() {
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.Exponential(100*time.Millisecond, 5*time.Second),
		MaxDelay:    2 * time.Second,
		OnRetry: func(attempt int, err error, nextWait time.Duration) {
			_ = attempt
			_ = err
			_ = nextWait
			// In production: log the retry attempt.
		},
	}
	_ = p
	fmt.Println("policy configured")
	// Output:
	// policy configured
}

// ExamplePolicy_Do_retryIf demonstrates selective retry based on error type.
func ExamplePolicy_Do_retryIf() {
	var ErrTransient = errors.New("transient")

	calls := 0
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.Constant(0),
		RetryIf: func(err error) bool {
			return errors.Is(err, ErrTransient)
		},
	}

	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		if calls == 1 {
			return ErrTransient
		}
		return nil
	})

	fmt.Printf("err=%v calls=%d\n", err, calls)
	// Output:
	// err=<nil> calls=2
}

// ExampleDoWithResult demonstrates the generic DoWithResult helper.
func ExampleDoWithResult() {
	calls := 0
	p := &retry.Policy{
		MaxAttempts: 3,
		Backoff:     backoff.Constant(0),
	}

	value, err := retry.DoWithResult(context.Background(), p, func(_ context.Context) (int, error) {
		calls++
		if calls < 2 {
			return 0, errors.New("not ready")
		}
		return 42, nil
	})

	fmt.Printf("value=%d err=%v\n", value, err)
	// Output:
	// value=42 err=<nil>
}

// ExamplePolicy_Do_fullJitter demonstrates the Full Jitter backoff strategy.
func ExamplePolicy_Do_fullJitter() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.FullJitter(100*time.Millisecond, 5*time.Second, rng),
	}
	_ = p
	fmt.Println("full jitter policy ready")
	// Output:
	// full jitter policy ready
}

// ExamplePolicy_Do_decorrelated demonstrates the AWS Decorrelated Jitter strategy.
func ExamplePolicy_Do_decorrelated() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	p := &retry.Policy{
		MaxAttempts: 8,
		Backoff:     backoff.Decorrelated(100*time.Millisecond, 10*time.Second, rng),
	}
	_ = p
	fmt.Println("decorrelated jitter policy ready")
	// Output:
	// decorrelated jitter policy ready
}
