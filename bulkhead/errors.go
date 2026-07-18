package bulkhead

import "errors"

// BulkheadError is a structured, inspectable error returned when a Bulkhead
// rejects a call because no slot was available. It wraps the ErrBulkheadFull
// sentinel, so errors.Is(err, ErrBulkheadFull) continues to work, while also
// exposing the bulkhead's name and saturation snapshot at the moment of
// rejection for richer observability and error-taxonomy handling.
//
// Extract it with errors.As or the AsBulkheadError helper:
//
//	if be, ok := bulkhead.AsBulkheadError(err); ok {
//	    log.Printf("bulkhead %q full: inflight=%d/%d waiting=%d",
//	        be.Name, be.Inflight, be.Capacity, be.Waiting)
//	}
type BulkheadError struct {
	// Name is the bulkhead's configured name.
	Name string
	// Capacity is the configured maximum concurrency (slot count).
	Capacity int
	// Inflight is the number of executions in progress at rejection time.
	Inflight int
	// Waiting is the queue depth (callers blocked awaiting a slot) at
	// rejection time.
	Waiting int
}

// Error implements the error interface.
func (e *BulkheadError) Error() string { return ErrBulkheadFull.Error() }

// Unwrap returns the ErrBulkheadFull sentinel so errors.Is(err, ErrBulkheadFull)
// matches a *BulkheadError.
func (e *BulkheadError) Unwrap() error { return ErrBulkheadFull }

// IsBulkheadFull reports whether err is (or wraps) ErrBulkheadFull.
func IsBulkheadFull(err error) bool { return errors.Is(err, ErrBulkheadFull) }

// AsBulkheadError extracts a *BulkheadError from err via errors.As, returning
// the value and true when present.
func AsBulkheadError(err error) (*BulkheadError, bool) {
	var be *BulkheadError
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}
