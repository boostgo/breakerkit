// Package breakerkit implements the circuit breaker pattern for protecting I/O resources
// (HTTP APIs, databases, message brokers, ...) from cascading failures.
//
// A [CircuitBreaker] is a state machine with three states:
//
//	        N failures in a row          OpenTimeout elapsed
//	Closed ─────────────────────► Open ─────────────────────► HalfOpen
//	  ▲                                                          │
//	  │               M successes in a row                       │
//	  └──────────────────────────────────────────────────────────┤
//	                                                             │ any failure
//	                                                             ▼
//	                                                            Open
//
// In [StateClosed] calls pass and failures are counted. In [StateOpen] every call is rejected
// immediately with [ErrCircuitOpen]. In [StateHalfOpen] a limited number of probe calls is
// admitted: enough consecutive successes close the circuit, any failure opens it again.
//
// Beyond the classic "N failures in a row" trigger the breaker supports a sliding-window
// failure ratio ([WithFailureRatio]), a predicate that separates business errors from resource
// failures ([WithIsFailure]), exactly-once transition callbacks ([WithOnStateChange]) and counter
// snapshots for metrics ([CircuitBreaker.Counts]). It has no dependencies outside the standard
// library and starts no goroutines.
//
// Basic usage:
//
//	cb := breakerkit.New(breakerkit.Config{FailureThreshold: 5, OpenTimeout: 30 * time.Second})
//
//	err := cb.Execute(ctx, func(ctx context.Context) error {
//		return client.Call(ctx, req)
//	})
//	if errors.Is(err, breakerkit.ErrCircuitOpen) {
//		// the resource is considered down: fail fast or serve a fallback
//	}
package breakerkit
