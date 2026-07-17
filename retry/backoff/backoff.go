// Package backoff provides composable backoff strategies for use with the retry package.
//
// All strategies implement the BackoffStrategy interface. The attempt parameter
// is 0-indexed: attempt=0 is the delay before the first retry (after the first failure).
package backoff

import "time"

// BackoffStrategy returns the delay before retry attempt n (0-indexed).
// n=0 is the first retry (after the first failure).
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}
