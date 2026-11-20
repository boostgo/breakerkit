package breakerkit

import "time"

// WithClock replaces the clock of the breaker; available to tests only.
func WithClock(now func() time.Time) Option {
	return func(cb *CircuitBreaker) { cb.now = now }
}
