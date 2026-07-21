package ratelimit

import "context"

// Peeker is the read-only subset of Limiter: the ability to observe a key's
// current state without consuming a token or otherwise mutating limiter state.
//
// It exists for interface segregation (ENHANCEMENTS §2.6): the full Limiter
// interface bundles mutating operations (Allow, AllowN, Wait, Reset, Close)
// with the read-only Peek. Consumers that must only observe — dashboards,
// exporters, metrics collectors, admission previews — should depend on Peeker
// instead of Limiter. This applies dependency inversion so an observer cannot
// accidentally (or maliciously) mutate the limiter it is watching, and follows
// the Go proverb "the bigger the interface, the weaker the abstraction".
//
// Every concrete limiter in this module already implements Peek, so it
// satisfies Peeker automatically; no limiter needs to change to be usable as a
// Peeker. See the compile-time assertions in observer_test.go.
type Peeker interface {
	// Peek returns the current state for key without consuming a token.
	Peek(ctx context.Context, key string) State
}

// Compile-time guarantee that the full Limiter interface is a superset of the
// read-only Peeker view, so any Limiter can be passed where a Peeker is expected.
var (
	_ Peeker = Limiter(nil)
)
