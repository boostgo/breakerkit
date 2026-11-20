package breakerkit

import "slices"

// State is the state of a [CircuitBreaker].
type State string

// States of a [CircuitBreaker].
const (
	// StateClosed is normal operation: calls pass, failures are counted.
	StateClosed State = "closed"
	// StateOpen rejects every call with [ErrCircuitOpen] until Config.OpenTimeout elapses.
	StateOpen State = "open"
	// StateHalfOpen admits a limited number of probe calls to test whether the resource recovered.
	StateHalfOpen State = "half_open"
)

// String returns the string value of the state ("closed", "open" or "half_open").
func (s State) String() string { return string(s) }

// Is reports whether s equals any of states.
func (s State) Is(states ...State) bool {
	return slices.Contains(states, s)
}

// Empty reports whether s is the zero value.
func (s State) Empty() bool {
	return s == ""
}
