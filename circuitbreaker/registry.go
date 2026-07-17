package circuitbreaker

import "sync"

// Registry manages a collection of named circuit breakers.
// All methods are safe for concurrent use.
type Registry struct {
	breakers sync.Map // name → *CircuitBreaker
	mu       sync.Mutex
}

// Global is the default shared registry.
var Global = NewRegistry()

// NewRegistry creates a new, empty circuit breaker registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// GetOrCreate returns the existing CircuitBreaker for name, or creates a new one
// with the given Config. Subsequent calls with the same name return the existing
// breaker regardless of the Config argument.
func (r *Registry) GetOrCreate(name string, cfg Config) *CircuitBreaker {
	if v, ok := r.breakers.Load(name); ok {
		return v.(*CircuitBreaker)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Double-check after acquiring lock
	if v, ok := r.breakers.Load(name); ok {
		return v.(*CircuitBreaker)
	}
	cfg.Name = name
	cb := New(cfg)
	r.breakers.Store(name, cb)
	return cb
}

// Get returns the CircuitBreaker for name, or nil if not found.
func (r *Registry) Get(name string) *CircuitBreaker {
	if v, ok := r.breakers.Load(name); ok {
		return v.(*CircuitBreaker)
	}
	return nil
}

// Snapshot returns a point-in-time snapshot of all registered breakers.
func (r *Registry) Snapshot() map[string]Snapshot {
	result := make(map[string]Snapshot)
	r.breakers.Range(func(key, value any) bool {
		cb := value.(*CircuitBreaker)
		result[cb.cfg.Name] = cb.Snapshot()
		return true
	})
	return result
}

// Delete removes a circuit breaker from the registry.
func (r *Registry) Delete(name string) {
	r.breakers.Delete(name)
}

// Reset clears all registered circuit breakers.
//
// Reset is best-effort with respect to concurrent GetOrCreate calls: it holds
// the same mutex GetOrCreate uses, so it will not race with breaker creation,
// but any GetOrCreate that completes after Reset returns may re-insert a breaker.
// It only guarantees that every breaker present when the lock was acquired is
// removed; it does not guarantee the registry is empty once other goroutines
// resume. Callers needing an atomically-empty registry should quiesce all
// GetOrCreate callers first.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breakers.Range(func(key, _ any) bool {
		r.breakers.Delete(key)
		return true
	})
}
