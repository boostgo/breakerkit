package breakerkit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Defaults that [New] uses instead of zero (or negative) fields of [Config].
const (
	// DefaultFailureThreshold is the default number of consecutive failures that open the circuit.
	DefaultFailureThreshold = 5
	// DefaultSuccessThreshold is the default number of consecutive HalfOpen successes that close it.
	DefaultSuccessThreshold = 2
	// DefaultOpenTimeout is the default time the circuit stays Open before moving to HalfOpen.
	DefaultOpenTimeout = 30 * time.Second
)

// Config holds the breaker parameters. A zero field means "not set": [New] uses the default
// instead (it does the same with negative values; use [Config.Validate] to reject them at startup).
type Config struct {
	// FailureThreshold is how many consecutive failures open the circuit.
	// Default: DefaultFailureThreshold (5).
	FailureThreshold int `json:"failure_threshold" yaml:"failureThreshold"`
	// SuccessThreshold is how many consecutive successes in HalfOpen close the circuit.
	// Default: DefaultSuccessThreshold (2).
	SuccessThreshold int `json:"success_threshold" yaml:"successThreshold"`
	// OpenTimeout is how long to stay Open before moving to HalfOpen.
	// Default: DefaultOpenTimeout (30s).
	OpenTimeout time.Duration `json:"open_timeout" yaml:"openTimeout"`
	// HalfOpenMaxProbes is how many probe calls may run at once in HalfOpen; 0 (default) means one.
	// Other calls get ErrCircuitOpen meanwhile: a barely recovered resource must not get the whole
	// worker pool at once. It has no effect on sequential (single goroutine) use.
	HalfOpenMaxProbes int `json:"half_open_max_probes" yaml:"halfOpenMaxProbes"`
}

// Validate checks the configuration, e.g. at service startup. Negative values yield
// [ErrInvalidConfig]; zero is not an error (it means "not set", see the defaults in [Config]).
func (c Config) Validate() error {
	switch {
	case c.FailureThreshold < 0:
		return fmt.Errorf("%w: failure_threshold must not be negative, got %d", ErrInvalidConfig, c.FailureThreshold)
	case c.SuccessThreshold < 0:
		return fmt.Errorf("%w: success_threshold must not be negative, got %d", ErrInvalidConfig, c.SuccessThreshold)
	case c.OpenTimeout < 0:
		return fmt.Errorf("%w: open_timeout must not be negative, got %s", ErrInvalidConfig, c.OpenTimeout)
	case c.HalfOpenMaxProbes < 0:
		return fmt.Errorf("%w: half_open_max_probes must not be negative, got %d", ErrInvalidConfig, c.HalfOpenMaxProbes)
	}
	return nil
}

// Counts is a snapshot of the breaker counters for metrics. All fields except Rejected are reset
// on every state transition.
type Counts struct {
	// Requests is the number of counted outcomes (successes + failures) since the last transition;
	// with WithFailureRatio, within the window. Ignored outcomes (caller cancellation) are not
	// counted.
	Requests uint64
	// Successes is the number of successful outcomes (same bounds as Requests).
	Successes uint64
	// Failures is the number of failed outcomes (same bounds as Requests).
	Failures uint64
	// ConsecutiveSuccesses is the current run of successes since the last transition (not windowed).
	ConsecutiveSuccesses uint64
	// ConsecutiveFailures is the current run of failures since the last transition (not windowed).
	ConsecutiveFailures uint64
	// Rejected is how many calls were rejected with ErrCircuitOpen over the breaker lifetime
	// (monotonic, never reset on transitions).
	Rejected uint64
}

// Option is an optional setting that does not belong to the (YAML/JSON) [Config].
type Option func(*CircuitBreaker)

// WithOnStateChange sets a callback invoked exactly once per actual state transition, no matter
// how many goroutines observe it. The callback runs synchronously but OUTSIDE the internal mutex:
// it does not block other Execute calls and may call the breaker methods. Keep it fast (push an
// alert into a buffer or channel instead of doing a synchronous HTTP call).
func WithOnStateChange(fn func(from, to State)) Option {
	return func(cb *CircuitBreaker) { cb.onStateChange = fn }
}

// WithIsFailure sets the predicate "is this error of fn a resource failure?". It is called only
// for err != nil and only when the outcome is not a caller cancellation (see Execute). false means
// the resource did answer and the error is a business-level one: such an outcome counts as a
// success (it resets the consecutive failure counter and counts toward a HalfOpen probe). By
// default every error is a failure. A nil predicate is ignored. The predicate runs outside the
// breaker mutex; its panic is treated like a panic in fn (see Execute).
func WithIsFailure(fn func(err error) bool) Option {
	return func(cb *CircuitBreaker) {
		if fn != nil {
			cb.isFailure = fn
		}
	}
}

// WithFailureRatio adds a failure-ratio trigger in Closed: the circuit opens when at least
// minRequests counted outcomes (successes + failures; cancelled calls are not counted) happened
// during the last window and failures/requests >= ratio. The FailureThreshold trigger
// ("N in a row") keeps working: whichever fires first opens the circuit.
//
// The window slides: it is split into 10 buckets and stale buckets are dropped by the breaker
// clock on calls (no background goroutines or tickers), so the window edge precision is window/10.
// The window counters are reset on every state transition.
//
// Invalid arguments (ratio outside (0, 1], minRequests < 1, window <= 0) leave the mode disabled.
func WithFailureRatio(ratio float64, minRequests int, window time.Duration) Option {
	return func(cb *CircuitBreaker) {
		if !(ratio > 0 && ratio <= 1) || minRequests < 1 || window <= 0 {
			return
		}
		cb.ratio = newRatioWindow(ratio, minRequests, window)
	}
}

type stateChange struct {
	from State
	to   State
}

// outcome is the classification of a call result for the breaker.
type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeFailure
	// outcomeIgnored affects neither the counters nor the state (the caller's ctx was cancelled).
	outcomeIgnored
)

// CircuitBreaker protects a single I/O resource (an HTTP API, a database, a queue, ...) from
// cascading failures. It is a state machine Closed -> Open -> HalfOpen -> Closed; see [State].
//
// A CircuitBreaker is safe for concurrent use by any number of goroutines. Create one instance
// per resource with [New]; the zero value is not usable.
type CircuitBreaker struct {
	cfg             Config
	state           State
	counts          Counts
	halfOpenProbes  int
	generation      uint64
	lastFailureTime time.Time
	pending         *stateChange
	onStateChange   func(from, to State)
	isFailure       func(err error) bool
	ratio           *ratioWindow
	now             func() time.Time
	mu              sync.Mutex
}

// New creates a breaker. Values <= 0 of FailureThreshold, SuccessThreshold and OpenTimeout are
// replaced with the defaults (see [Config]); New never panics and never returns an error. Use
// [Config.Validate] to check a configuration at startup.
func New(cfg Config, opts ...Option) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = DefaultFailureThreshold
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = DefaultSuccessThreshold
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = DefaultOpenTimeout
	}

	cb := &CircuitBreaker{
		cfg:       cfg,
		state:     StateClosed,
		isFailure: func(error) bool { return true },
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(cb)
	}
	return cb
}

// Execute runs fn under the protection of the breaker and returns its error as is (or
// [ErrCircuitOpen] if the call is not admitted). The outcome is classified as follows:
//   - err == nil is a success;
//   - ctx.Err() != nil and err is context.Canceled/DeadlineExceeded: the caller gave up, the
//     outcome is ignored (counters and state do not change, the HalfOpen probe slot is released).
//     DeadlineExceeded from fn's own timeout while ctx is still alive is a failure;
//   - otherwise the [WithIsFailure] predicate decides whether it is a failure or a success.
//
// A panic in fn (or in the predicate) counts as a failure (a crashed probe does not prove the
// resource is healthy), the probe slot is released and the panic is re-raised (the stack of the
// original panic is not preserved in the new one). runtime.Goexit in fn ignores the outcome.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	generation, err := cb.beforeCall()
	if err != nil {
		return err
	}

	finished := false
	defer func() {
		if finished {
			return
		}
		r := recover()
		result := outcomeIgnored // Goexit: recover() == nil
		if r != nil {
			result = outcomeFailure
		}
		cb.afterCall(generation, result)
		if r != nil {
			panic(r)
		}
	}()

	callErr := fn(ctx)
	result := cb.classify(ctx, callErr)
	finished = true

	cb.afterCall(generation, result)

	return callErr
}

// classify runs outside the mutex: the predicate may be slow or panic.
func (cb *CircuitBreaker) classify(ctx context.Context, err error) outcome {
	switch {
	case err == nil:
		return outcomeSuccess
	case ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		return outcomeIgnored
	case cb.isFailure(err):
		return outcomeFailure
	default:
		return outcomeSuccess
	}
}

// State returns the current state, e.g. for metrics. To alert on transitions use
// [WithOnStateChange] instead of comparing State before and after a call.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Counts returns a snapshot of the counters taken under the mutex, e.g. for metrics. The counters
// are reset on every state transition (except Rejected), see [Counts].
func (cb *CircuitBreaker) Counts() Counts {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	c := cb.counts
	if cb.ratio != nil {
		c.Successes, c.Failures = cb.ratio.totals(cb.now())
		c.Requests = c.Successes + c.Failures
	}
	return c
}

// beforeCall decides whether to admit the call and returns the state generation in which it was
// admitted. A result that comes back in another generation (the circuit switched while the I/O
// was in flight) does not belong to the current state and is dropped in afterCall.
func (cb *CircuitBreaker) beforeCall() (uint64, error) {
	cb.mu.Lock()

	if cb.state == StateOpen {
		if cb.now().Sub(cb.lastFailureTime) < cb.cfg.OpenTimeout {
			cb.counts.Rejected++
			cb.mu.Unlock()
			return 0, ErrCircuitOpen
		}
		cb.transitionTo(StateHalfOpen)
	}

	if cb.state == StateHalfOpen && cb.halfOpenProbes >= cb.halfOpenMaxProbes() {
		cb.counts.Rejected++
		change := cb.drainChange()
		cb.mu.Unlock()
		cb.notify(change)
		return 0, ErrCircuitOpen
	}

	if cb.state == StateHalfOpen {
		cb.halfOpenProbes++
	}

	generation := cb.generation
	change := cb.drainChange()
	cb.mu.Unlock()
	cb.notify(change)

	return generation, nil
}

func (cb *CircuitBreaker) afterCall(generation uint64, result outcome) {
	cb.mu.Lock()

	if generation != cb.generation {
		// The state changed while the call was in flight: its outcome belongs to a past generation.
		// No probe slot to release: halfOpenProbes is reset on entering HalfOpen.
		cb.mu.Unlock()
		return
	}

	if cb.state == StateHalfOpen && cb.halfOpenProbes > 0 {
		cb.halfOpenProbes--
	}

	switch result {
	case outcomeFailure:
		cb.onFailure()
	case outcomeSuccess:
		cb.onSuccess()
	case outcomeIgnored:
		// Neither a success nor a failure: counters and state stay unchanged.
	}

	change := cb.drainChange()
	cb.mu.Unlock()
	cb.notify(change)
}

func (cb *CircuitBreaker) onFailure() {
	cb.counts.Requests++
	cb.counts.Failures++
	cb.counts.ConsecutiveFailures++
	cb.counts.ConsecutiveSuccesses = 0

	switch cb.state {
	case StateClosed:
		if cb.ratio != nil {
			cb.ratio.record(cb.now(), false)
		}
		if cb.counts.ConsecutiveFailures >= toUint64(cb.cfg.FailureThreshold) || cb.ratioExceeded() {
			cb.transitionTo(StateOpen)
		}
	case StateHalfOpen:
		cb.transitionTo(StateOpen)
	}
}

func (cb *CircuitBreaker) onSuccess() {
	cb.counts.Requests++
	cb.counts.Successes++
	cb.counts.ConsecutiveSuccesses++
	cb.counts.ConsecutiveFailures = 0

	switch cb.state {
	case StateClosed:
		if cb.ratio != nil {
			cb.ratio.record(cb.now(), true)
			if cb.ratioExceeded() {
				cb.transitionTo(StateOpen)
			}
		}
	case StateHalfOpen:
		if cb.counts.ConsecutiveSuccesses >= toUint64(cb.cfg.SuccessThreshold) {
			cb.transitionTo(StateClosed)
		}
	}
}

// ratioExceeded checks the failure ratio trigger; called under cb.mu.
func (cb *CircuitBreaker) ratioExceeded() bool {
	if cb.ratio == nil {
		return false
	}
	successes, failures := cb.ratio.totals(cb.now())
	return cb.ratio.exceeded(successes, failures)
}

// transitionTo moves the state machine to next and makes the counters consistent. It is called
// only under cb.mu and never invokes the callback itself: the transition is stored in cb.pending
// and handed to the caller, which notifies after Unlock.
func (cb *CircuitBreaker) transitionTo(next State) {
	if cb.state == next {
		return
	}

	from := cb.state
	cb.state = next
	cb.generation++

	// Rejected is monotonic, everything else is reset on every transition.
	cb.counts = Counts{Rejected: cb.counts.Rejected}
	if cb.ratio != nil {
		cb.ratio.reset()
	}

	switch next {
	case StateOpen:
		cb.lastFailureTime = cb.now()
	case StateHalfOpen:
		cb.halfOpenProbes = 0
	case StateClosed:
	}

	cb.pending = &stateChange{from: from, to: next}
}

func (cb *CircuitBreaker) drainChange() *stateChange {
	change := cb.pending
	cb.pending = nil
	return change
}

func (cb *CircuitBreaker) notify(change *stateChange) {
	if change == nil || cb.onStateChange == nil {
		return
	}
	cb.onStateChange(change.from, change.to)
}

func (cb *CircuitBreaker) halfOpenMaxProbes() int {
	if cb.cfg.HalfOpenMaxProbes < 1 {
		return 1
	}
	return cb.cfg.HalfOpenMaxProbes
}

// toUint64 converts a threshold that is known to be positive (New replaces values <= 0).
func toUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v) //nolint:gosec // v is checked non-negative above
}
