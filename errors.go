package breakerkit

import "errors"

var (
	// ErrCircuitOpen is returned by [CircuitBreaker.Execute] when the call is rejected because
	// the circuit is open (or the half-open probe limit is reached). fn is not invoked.
	ErrCircuitOpen = errors.New("circuit_breaker.circuit_open")
	// ErrInvalidConfig is returned by [Config.Validate] for invalid (negative) values.
	ErrInvalidConfig = errors.New("circuit_breaker.invalid_config")
)
