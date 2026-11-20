package breakerkit_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boostgo/breakerkit"
)

var errBoom = errors.New("boom")

func fail(context.Context) error { return errBoom }

func ok(context.Context) error { return nil }

// --- single-goroutine invariants (regression guard for concurrency changes) ---

func TestClosedToOpenAfterThreshold(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      time.Minute,
	})

	for i := range 2 {
		if err := cb.Execute(context.Background(), fail); !errors.Is(err, errBoom) {
			t.Fatalf("call %d: want errBoom, got %v", i, err)
		}
		if cb.State() != breakerkit.StateClosed {
			t.Fatalf("call %d: want closed, got %s", i, cb.State())
		}
	}

	if err := cb.Execute(context.Background(), fail); !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
	if cb.State() != breakerkit.StateOpen {
		t.Fatalf("want open, got %s", cb.State())
	}

	// An open circuit rejects the call without invoking fn.
	called := false
	err := cb.Execute(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, breakerkit.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	if called {
		t.Fatal("fn must not be called while circuit is open")
	}
}

func TestSuccessResetsFailureCountInClosed(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 3,
		SuccessThreshold: 1,
		OpenTimeout:      time.Minute,
	})

	for range 5 {
		_ = cb.Execute(context.Background(), fail)
		_ = cb.Execute(context.Background(), fail)
		_ = cb.Execute(context.Background(), ok)
	}

	if cb.State() != breakerkit.StateClosed {
		t.Fatalf("want closed (the threshold counts consecutive failures), got %s", cb.State())
	}
}

func TestOpenToHalfOpenToClosed(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 1,
		SuccessThreshold: 2,
		OpenTimeout:      10 * time.Millisecond,
	})

	_ = cb.Execute(context.Background(), fail)
	if cb.State() != breakerkit.StateOpen {
		t.Fatalf("want open, got %s", cb.State())
	}

	time.Sleep(20 * time.Millisecond)

	if err := cb.Execute(context.Background(), ok); err != nil {
		t.Fatalf("probe must pass after timeout: %v", err)
	}
	if cb.State() != breakerkit.StateHalfOpen {
		t.Fatalf("want half_open after 1/2 successes, got %s", cb.State())
	}

	if err := cb.Execute(context.Background(), ok); err != nil {
		t.Fatalf("second probe: %v", err)
	}
	if cb.State() != breakerkit.StateClosed {
		t.Fatalf("want closed after 2/2 successes, got %s", cb.State())
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 1,
		SuccessThreshold: 5,
		OpenTimeout:      10 * time.Millisecond,
	})

	_ = cb.Execute(context.Background(), fail)
	time.Sleep(20 * time.Millisecond)

	_ = cb.Execute(context.Background(), ok)
	if cb.State() != breakerkit.StateHalfOpen {
		t.Fatalf("want half_open, got %s", cb.State())
	}

	_ = cb.Execute(context.Background(), fail)
	if cb.State() != breakerkit.StateOpen {
		t.Fatalf("want open, got %s", cb.State())
	}
	// The open timer was restarted: no probe is admitted right away.
	if err := cb.Execute(context.Background(), ok); !errors.Is(err, breakerkit.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
}

// With a single goroutine the probe limit must change nothing: calls go one by one.
func TestHalfOpenProbeLimitDoesNotAffectSequentialCalls(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 1,
		SuccessThreshold: 4,
		OpenTimeout:      10 * time.Millisecond,
	})

	_ = cb.Execute(context.Background(), fail)
	time.Sleep(20 * time.Millisecond)

	for i := range 4 {
		if err := cb.Execute(context.Background(), ok); err != nil {
			t.Fatalf("sequential probe %d rejected: %v", i, err)
		}
	}
	if cb.State() != breakerkit.StateClosed {
		t.Fatalf("want closed, got %s", cb.State())
	}
}

// --- concurrency ---

// Main race test: N goroutines x M calls with a schedule of successes/failures, invariants checked.
func TestConcurrentExecuteRace(t *testing.T) {
	const (
		goroutines = 32
		calls      = 200
	)

	var transitions int64

	cb := breakerkit.New(
		breakerkit.Config{
			FailureThreshold: 5,
			SuccessThreshold: 3,
			OpenTimeout:      time.Millisecond,
		},
		breakerkit.WithOnStateChange(func(from, to breakerkit.State) {
			if from == to {
				t.Errorf("degenerate transition %s -> %s", from, to)
			}
			if to.Empty() || !to.Is(breakerkit.StateClosed, breakerkit.StateOpen, breakerkit.StateHalfOpen) {
				t.Errorf("unknown target state %q", to)
			}
			atomic.AddInt64(&transitions, 1)
		}),
	)

	var seq int64
	var wg sync.WaitGroup

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range calls {
				n := atomic.AddInt64(&seq, 1)
				err := cb.Execute(context.Background(), func(context.Context) error {
					// schedule: series of 7 failures, then 7 successes, and so on.
					if (n/7)%2 == 0 {
						return errBoom
					}
					return nil
				})
				switch {
				case err == nil, errors.Is(err, errBoom), errors.Is(err, breakerkit.ErrCircuitOpen):
				default:
					t.Errorf("unexpected error: %v", err)
					return
				}
				if s := cb.State(); !s.Is(breakerkit.StateClosed, breakerkit.StateOpen, breakerkit.StateHalfOpen) {
					t.Errorf("invalid state %q", s)
					return
				}
			}
		}()
	}

	wg.Wait()

	if atomic.LoadInt64(&transitions) == 0 {
		t.Fatal("expected at least one state transition under this schedule")
	}
	if s := cb.State(); !s.Is(breakerkit.StateClosed, breakerkit.StateOpen, breakerkit.StateHalfOpen) {
		t.Fatalf("invalid final state %q", s)
	}
}

// State() must be safe to call concurrently with Execute() (a pure -race check).
func TestConcurrentStateAndExecuteRace(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      time.Millisecond,
	})

	stop := make(chan struct{})

	var readers, writers sync.WaitGroup

	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = cb.State()
					runtime.Gosched()
				}
			}
		}()
	}

	var n int64
	for range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for range 500 {
				_ = cb.Execute(context.Background(), func(context.Context) error {
					if atomic.AddInt64(&n, 1)%3 == 0 {
						return errBoom
					}
					return nil
				})
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()
}

// In HalfOpen at most HalfOpenMaxProbes calls may run at once, not the whole worker pool.
func TestHalfOpenLimitsConcurrentProbes(t *testing.T) {
	const (
		goroutines = 16
		maxProbes  = 2
	)

	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold:  1,
		SuccessThreshold:  1000, // the circuit will not close during the measurement
		OpenTimeout:       10 * time.Millisecond,
		HalfOpenMaxProbes: maxProbes,
	})

	_ = cb.Execute(context.Background(), fail)
	if cb.State() != breakerkit.StateOpen {
		t.Fatalf("want open, got %s", cb.State())
	}
	time.Sleep(20 * time.Millisecond)

	var inFlight, peak, admitted, rejected int64

	var wg sync.WaitGroup
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := cb.Execute(context.Background(), func(context.Context) error {
				cur := atomic.AddInt64(&inFlight, 1)
				for {
					old := atomic.LoadInt64(&peak)
					if cur <= old || atomic.CompareAndSwapInt64(&peak, old, cur) {
						break
					}
				}
				atomic.AddInt64(&admitted, 1)
				time.Sleep(20 * time.Millisecond) // hold the slot so that all goroutines collide
				atomic.AddInt64(&inFlight, -1)
				return nil
			})
			if errors.Is(err, breakerkit.ErrCircuitOpen) {
				atomic.AddInt64(&rejected, 1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if p := atomic.LoadInt64(&peak); p > maxProbes {
		t.Fatalf("half-open admitted %d concurrent probes, limit is %d", p, maxProbes)
	}
	if atomic.LoadInt64(&admitted) == 0 {
		t.Fatal("half-open must admit at least one probe")
	}
	if atomic.LoadInt64(&rejected) == 0 {
		t.Fatal("half-open must reject the rest of the pool with ErrCircuitOpen")
	}
}

// One actual Closed -> Open transition means exactly one callback invocation, no matter how
// many goroutines observe the failure at the same time.
func TestOnStateChangeFiresOncePerTransition(t *testing.T) {
	const goroutines = 32

	var opened int64
	cb := breakerkit.New(
		breakerkit.Config{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Hour, // never leaves Open in this test
		},
		breakerkit.WithOnStateChange(func(_, to breakerkit.State) {
			if to == breakerkit.StateOpen {
				atomic.AddInt64(&opened, 1)
			}
		}),
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = cb.Execute(context.Background(), fail)
		}()
	}

	close(start)
	wg.Wait()

	if got := atomic.LoadInt64(&opened); got != 1 {
		t.Fatalf("want exactly 1 open transition, got %d", got)
	}
}

// The callback runs outside the mutex: calling the breaker from the callback must not deadlock.
func TestOnStateChangeMayCallBreaker(t *testing.T) {
	var cb *breakerkit.CircuitBreaker
	seen := make(chan breakerkit.State, 1)

	cb = breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour},
		breakerkit.WithOnStateChange(func(_, _ breakerkit.State) { seen <- cb.State() }),
	)

	done := make(chan struct{})
	go func() {
		_ = cb.Execute(context.Background(), fail)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: OnStateChange is invoked while holding the mutex")
	}

	if got := <-seen; got != breakerkit.StateOpen {
		t.Fatalf("want open inside callback, got %s", got)
	}
}

// Naive transition detection on the caller side (prev := State(); Execute(); State()) produces
// duplicates under concurrency: every goroutine sees the same single transition.
func TestNaivePrevStateDetectionDuplicatesUnderConcurrency(t *testing.T) {
	const goroutines = 16

	var callbackOpens, naiveOpens int64
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour},
		breakerkit.WithOnStateChange(func(_, to breakerkit.State) {
			if to == breakerkit.StateOpen {
				atomic.AddInt64(&callbackOpens, 1)
			}
		}),
	)

	var wg, ready sync.WaitGroup
	barrier := make(chan struct{})

	ready.Add(goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prev := cb.State() // everyone reads Closed: no failure has happened yet
			ready.Done()
			<-barrier
			_ = cb.Execute(context.Background(), fail)
			if prev != breakerkit.StateOpen && cb.State() == breakerkit.StateOpen {
				atomic.AddInt64(&naiveOpens, 1)
			}
		}()
	}

	ready.Wait() // everyone has read prev before anyone reaches Execute
	close(barrier)
	wg.Wait()

	if got := atomic.LoadInt64(&callbackOpens); got != 1 {
		t.Fatalf("callback: want exactly 1 open, got %d", got)
	}
	if got := atomic.LoadInt64(&naiveOpens); got != goroutines {
		t.Fatalf("naive detection: want %d duplicate alerts (demonstration), got %d", goroutines, got)
	}
}

// The outcome of a call that returns in another state generation must not be applied to the
// current state: a stale failure from the outage must not instantly re-open a fresh circuit.
func TestStaleOutcomeDoesNotAffectNewGeneration(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      10 * time.Millisecond,
	})

	release := make(chan struct{})
	slowDone := make(chan struct{})

	// A slow call starts in Closed and returns an error after the circuit has already
	// been opened (by another goroutine) and has moved on.
	go func() {
		defer close(slowDone)
		_ = cb.Execute(context.Background(), func(context.Context) error {
			<-release
			return errBoom
		})
	}()

	// Wait until the slow call is surely admitted.
	time.Sleep(20 * time.Millisecond)

	_ = cb.Execute(context.Background(), fail) // Closed -> Open
	if cb.State() != breakerkit.StateOpen {
		t.Fatalf("want open, got %s", cb.State())
	}
	time.Sleep(20 * time.Millisecond)

	if err := cb.Execute(context.Background(), ok); err != nil { // Open -> HalfOpen -> Closed
		t.Fatalf("probe: %v", err)
	}
	if cb.State() != breakerkit.StateClosed {
		t.Fatalf("want closed, got %s", cb.State())
	}

	close(release)
	<-slowDone

	if cb.State() != breakerkit.StateClosed {
		t.Fatalf("stale failure re-opened the circuit: %s", cb.State())
	}
}

// --- failure classification, ratio window, counts ---

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var errBusiness = errors.New("business")

func isNotBusiness(err error) bool { return !errors.Is(err, errBusiness) }

func mustState(t *testing.T, cb *breakerkit.CircuitBreaker, want breakerkit.State) {
	t.Helper()
	if got := cb.State(); got != want {
		t.Fatalf("want state %s, got %s", want, got)
	}
}

func TestExecute_IsFailurePredicate(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 3, SuccessThreshold: 2, OpenTimeout: time.Minute},
		breakerkit.WithClock(clock.Now),
		breakerkit.WithIsFailure(isNotBusiness),
	)
	business := func(context.Context) error { return errBusiness }

	for range 10 {
		if err := cb.Execute(context.Background(), business); !errors.Is(err, errBusiness) {
			t.Fatalf("want errBusiness returned as is, got %v", err)
		}
	}
	mustState(t, cb, breakerkit.StateClosed)

	// A business error resets the consecutive failure counter.
	_ = cb.Execute(context.Background(), fail)
	_ = cb.Execute(context.Background(), fail)
	_ = cb.Execute(context.Background(), business)
	_ = cb.Execute(context.Background(), fail)
	_ = cb.Execute(context.Background(), fail)
	mustState(t, cb, breakerkit.StateClosed)

	_ = cb.Execute(context.Background(), fail)
	mustState(t, cb, breakerkit.StateOpen)

	// In HalfOpen a business error counts as a success.
	clock.Advance(time.Minute)
	_ = cb.Execute(context.Background(), business)
	mustState(t, cb, breakerkit.StateHalfOpen)
	_ = cb.Execute(context.Background(), business)
	mustState(t, cb, breakerkit.StateClosed)
}

func TestExecute_CallerCanceledNotFailure(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{FailureThreshold: 2, SuccessThreshold: 1, OpenTimeout: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 5 {
		err := cb.Execute(ctx, func(ctx context.Context) error { return ctx.Err() })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	}
	mustState(t, cb, breakerkit.StateClosed)
	if got := cb.Counts(); got != (breakerkit.Counts{}) {
		t.Fatalf("counts must not change, got %+v", got)
	}

	// The caller's deadline is neutral as well.
	dctx, dcancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer dcancel()
	_ = cb.Execute(dctx, func(ctx context.Context) error { return ctx.Err() })
	mustState(t, cb, breakerkit.StateClosed)
	if got := cb.Counts(); got != (breakerkit.Counts{}) {
		t.Fatalf("counts must not change, got %+v", got)
	}
}

func TestExecute_OwnDeadlineIsFailure(t *testing.T) {
	cb := breakerkit.New(breakerkit.Config{FailureThreshold: 2, SuccessThreshold: 1, OpenTimeout: time.Minute})
	// The caller's ctx is alive; fn returned DeadlineExceeded from its own timeout.
	own := func(context.Context) error { return context.DeadlineExceeded }

	_ = cb.Execute(context.Background(), own)
	_ = cb.Execute(context.Background(), own)
	mustState(t, cb, breakerkit.StateOpen)
}

func TestExecute_CanceledInHalfOpenReleasesSlot(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithClock(clock.Now),
	)
	_ = cb.Execute(context.Background(), fail)
	clock.Advance(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = cb.Execute(ctx, func(ctx context.Context) error { return ctx.Err() })
	mustState(t, cb, breakerkit.StateHalfOpen)

	// The slot is free: the next probe is admitted and closes the circuit.
	if err := cb.Execute(context.Background(), ok); err != nil {
		t.Fatalf("probe must be admitted, got %v", err)
	}
	mustState(t, cb, breakerkit.StateClosed)
}

func TestExecute_PanicReleasesProbe(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithClock(clock.Now),
	)
	_ = cb.Execute(context.Background(), fail)
	clock.Advance(time.Minute)

	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Fatalf("panic must be rethrown, got %v", r)
			}
		}()
		_ = cb.Execute(context.Background(), func(context.Context) error { panic("kaboom") })
	}()
	// The panic counts as a failed probe: the circuit is Open again.
	mustState(t, cb, breakerkit.StateOpen)

	clock.Advance(time.Minute)
	if err := cb.Execute(context.Background(), ok); err != nil {
		t.Fatalf("probe must be admitted after OpenTimeout, got %v", err)
	}
	mustState(t, cb, breakerkit.StateClosed)
}

func TestExecute_PanicConcurrent(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Minute, HalfOpenMaxProbes: 2},
		breakerkit.WithClock(clock.Now),
	)
	_ = cb.Execute(context.Background(), fail)
	clock.Advance(time.Minute)

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = recover() }()
			_ = cb.Execute(context.Background(), func(context.Context) error { panic("kaboom") })
		}()
	}
	wg.Wait()

	clock.Advance(time.Minute)
	if err := cb.Execute(context.Background(), ok); err != nil {
		t.Fatalf("breaker must not be stuck, got %v", err)
	}
}

func TestExecute_PredicatePanicDoesNotLockOrStick(t *testing.T) {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 5, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithIsFailure(func(error) bool { panic("predicate") }),
	)
	func() {
		defer func() { _ = recover() }()
		_ = cb.Execute(context.Background(), fail)
	}()
	// The mutex is not held: State() does not block.
	mustState(t, cb, breakerkit.StateClosed)
	if got := cb.Counts().Failures; got != 1 {
		t.Fatalf("panic counted as failure, got %d", got)
	}
}

func TestNew_ZeroConfigDefaults(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(breakerkit.Config{}, breakerkit.WithClock(clock.Now))

	_ = cb.Execute(context.Background(), fail)
	mustState(t, cb, breakerkit.StateClosed) // does not open on the first failure

	for range breakerkit.DefaultFailureThreshold - 1 {
		_ = cb.Execute(context.Background(), fail)
	}
	mustState(t, cb, breakerkit.StateOpen)

	// HalfOpen is not instant.
	if err := cb.Execute(context.Background(), ok); !errors.Is(err, breakerkit.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	clock.Advance(breakerkit.DefaultOpenTimeout)

	// Does not close on the first success.
	_ = cb.Execute(context.Background(), ok)
	mustState(t, cb, breakerkit.StateHalfOpen)
	_ = cb.Execute(context.Background(), ok)
	mustState(t, cb, breakerkit.StateClosed)
}

func TestConfig_ValidateNegative(t *testing.T) {
	tests := []struct {
		name    string
		cfg     breakerkit.Config
		wantErr bool
	}{
		{"zero is not an error", breakerkit.Config{}, false},
		{"valid", breakerkit.Config{FailureThreshold: 5, SuccessThreshold: 2, OpenTimeout: time.Second, HalfOpenMaxProbes: 1}, false},
		{"negative failure threshold", breakerkit.Config{FailureThreshold: -1}, true},
		{"negative success threshold", breakerkit.Config{SuccessThreshold: -1}, true},
		{"negative open timeout", breakerkit.Config{OpenTimeout: -time.Second}, true},
		{"negative probes", breakerkit.Config{HalfOpenMaxProbes: -1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != errors.Is(err, breakerkit.ErrInvalidConfig) || (err != nil) != tt.wantErr {
				t.Fatalf("wantErr=%v, got %v", tt.wantErr, err)
			}
		})
	}
}

func ratioBreaker(clock *fakeClock) *breakerkit.CircuitBreaker {
	return breakerkit.New(
		breakerkit.Config{FailureThreshold: 100, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithClock(clock.Now),
		breakerkit.WithFailureRatio(0.5, 10, time.Minute),
	)
}

func TestFailureRatio_OpensOnRatio(t *testing.T) {
	cb := ratioBreaker(newFakeClock())
	for i := range 10 {
		if i%2 == 0 {
			_ = cb.Execute(context.Background(), fail)
		} else {
			_ = cb.Execute(context.Background(), ok)
		}
	}
	mustState(t, cb, breakerkit.StateOpen)
}

func TestFailureRatio_BelowMinRequests(t *testing.T) {
	cb := ratioBreaker(newFakeClock())
	for range 4 {
		_ = cb.Execute(context.Background(), fail)
		_ = cb.Execute(context.Background(), ok)
	}
	_ = cb.Execute(context.Background(), fail) // 9 outcomes, 5 failures: still below minRequests
	mustState(t, cb, breakerkit.StateClosed)
}

func TestFailureRatio_WindowSlides(t *testing.T) {
	clock := newFakeClock()
	cb := ratioBreaker(clock)
	for range 4 {
		_ = cb.Execute(context.Background(), fail)
		_ = cb.Execute(context.Background(), ok)
	}
	clock.Advance(2 * time.Minute) // old outcomes left the window
	_ = cb.Execute(context.Background(), fail)
	_ = cb.Execute(context.Background(), ok)
	mustState(t, cb, breakerkit.StateClosed)
	if got := cb.Counts().Requests; got != 2 {
		t.Fatalf("want 2 requests in window, got %d", got)
	}
}

func TestFailureRatio_ConsecutiveStillWorks(t *testing.T) {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 3, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithClock(newFakeClock().Now),
		breakerkit.WithFailureRatio(0.9, 100, time.Minute),
	)
	for range 3 {
		_ = cb.Execute(context.Background(), fail)
	}
	mustState(t, cb, breakerkit.StateOpen)
}

func TestFailureRatio_InvalidArgsDisable(t *testing.T) {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 100, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithFailureRatio(0, 1, time.Minute),
		breakerkit.WithFailureRatio(1.5, 1, time.Minute),
		breakerkit.WithFailureRatio(0.5, 0, time.Minute),
		breakerkit.WithFailureRatio(0.5, 1, 0),
	)
	for range 20 {
		_ = cb.Execute(context.Background(), fail)
	}
	mustState(t, cb, breakerkit.StateClosed)
}

func TestCounts_Snapshot(t *testing.T) {
	clock := newFakeClock()
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 3, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithClock(clock.Now),
	)
	_ = cb.Execute(context.Background(), ok)
	_ = cb.Execute(context.Background(), ok)
	_ = cb.Execute(context.Background(), fail)
	want := breakerkit.Counts{Requests: 3, Successes: 2, Failures: 1, ConsecutiveFailures: 1}
	if got := cb.Counts(); got != want {
		t.Fatalf("want %+v, got %+v", want, got)
	}

	_ = cb.Execute(context.Background(), fail)
	_ = cb.Execute(context.Background(), fail)
	mustState(t, cb, breakerkit.StateOpen)
	_ = cb.Execute(context.Background(), ok) // rejected
	want = breakerkit.Counts{Rejected: 1}
	if got := cb.Counts(); got != want {
		t.Fatalf("counts reset on transition, rejected kept: want %+v, got %+v", want, got)
	}
}

func TestCounts_ConcurrentWithExecute(t *testing.T) {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1000, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithFailureRatio(0.9, 10, time.Minute),
	)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 200 {
				if i%2 == 0 {
					_ = cb.Execute(context.Background(), ok)
				} else {
					_ = cb.Execute(context.Background(), fail)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 200 {
				_ = cb.Counts()
				_ = cb.State()
			}
		}()
	}
	wg.Wait()
}
