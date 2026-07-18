// Package resiliencex provides generic, value-returning adapters over the core
// resilience primitives in this module (circuit breaker, retry, and bulkhead).
//
// The primitives themselves are built around the Go-idiomatic
// func(context.Context) error execution shape, which keeps their internals
// simple and allocation-light. That shape, however, forces callers who need to
// return a typed result to close over an out-of-band variable:
//
//	var user User
//	err := cb.Execute(ctx, func(ctx context.Context) error {
//	    var e error
//	    user, e = fetchUser(ctx, id)
//	    return e
//	})
//
// resiliencex removes that boilerplate with thin generic wrappers that accept a
// func(context.Context) (T, error) and return (T, error):
//
//	user, err := resiliencex.ExecuteCB(ctx, cb, func(ctx context.Context) (User, error) {
//	    return fetchUser(ctx, id)
//	})
//
// # Semantics
//
// Each wrapper is a faithful adapter: it delegates to the underlying primitive
// unchanged and returns that primitive's error verbatim (no wrapping, no
// translation). Sentinel checks therefore keep working through the wrapper, for
// example errors.Is(err, circuitbreaker.ErrCircuitOpen),
// errors.Is(err, bulkhead.ErrBulkheadFull), or a context error from a cancelled
// ctx. On any error the wrapper returns the zero value of T alongside that
// error; the typed value is meaningful only when err is nil.
//
// The wrappers add no synchronization of their own and are exactly as safe for
// concurrent use as the primitive they wrap. They introduce no external
// dependencies beyond this module and the standard library.
package resiliencex
