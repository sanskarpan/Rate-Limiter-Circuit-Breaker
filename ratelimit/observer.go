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

// Observer is the read-only view a monitoring consumer should depend on. It is
// currently identical to Peeker and is provided as a distinct, intention-
// revealing name for the dashboard/metrics use case: code that accepts an
// Observer documents that it only ever reads limiter state.
//
// Observer is kept separate from Peeker so additional read-only accessors can
// be added here in the future (for example a bounded key enumerator) without
// widening Peeker, which is meant to stay minimal. Any Limiter — and any
// Peeker that also gains those future methods — satisfies Observer.
type Observer interface {
	// Peek returns the current state for key without consuming a token.
	Peek(ctx context.Context, key string) State
}

// Compile-time guarantee that the full Limiter interface is a superset of the
// read-only views, so any Limiter can be passed where a Peeker or Observer is
// expected.
var (
	_ Peeker   = Limiter(nil)
	_ Observer = Limiter(nil)
)
