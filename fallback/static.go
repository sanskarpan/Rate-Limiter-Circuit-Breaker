package fallback

import "context"

// Static returns a fallback function that always yields the fixed value v
// (with a nil error) on primary failure. It is a convenience for the common
// case of "serve a fixed default when the primary fails" and pairs with
// DoWithResult:
//
//	user, err := fallback.DoWithResult(ctx, fetchUser, fallback.Static(defaultUser))
//
// The returned function ignores the cause error; use a custom fallback if you
// need to inspect it.
func Static[T any](v T) func(context.Context, error) (T, error) {
	return func(context.Context, error) (T, error) {
		return v, nil
	}
}

// StaticErr is like Static but preserves the original error alongside the fixed
// value. Use it when a caller wants the default value but still needs to observe
// (log, meter) that the primary failed.
func StaticErr[T any](v T) func(context.Context, error) (T, error) {
	return func(_ context.Context, cause error) (T, error) {
		return v, cause
	}
}
