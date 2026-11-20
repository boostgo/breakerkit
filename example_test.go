package breakerkit_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/boostgo/breakerkit"
)

func ExampleNew() {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	ctx := context.Background()
	errDown := errors.New("connection refused")

	for range 2 {
		_ = cb.Execute(ctx, func(context.Context) error { return errDown })
	}
	fmt.Println("state:", cb.State())

	called := false
	err := cb.Execute(ctx, func(context.Context) error { called = true; return nil })
	fmt.Println(errors.Is(err, breakerkit.ErrCircuitOpen), called)

	// Output:
	// state: open
	// true false
}

func ExampleWithIsFailure() {
	// A 4xx-like rejection means the payments API is alive: it must not open the circuit.
	errRejected := errors.New("payment rejected")

	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 2, SuccessThreshold: 1, OpenTimeout: 30 * time.Second},
		breakerkit.WithIsFailure(func(err error) bool { return !errors.Is(err, errRejected) }),
	)
	ctx := context.Background()

	for range 5 {
		_ = cb.Execute(ctx, func(context.Context) error { return errRejected })
	}
	fmt.Println("after business errors:", cb.State())

	errDown := errors.New("unavailable")
	for range 2 {
		_ = cb.Execute(ctx, func(context.Context) error { return errDown })
	}
	fmt.Println("after resource failures:", cb.State())

	// Output:
	// after business errors: closed
	// after resource failures: open
}

func ExampleWithFailureRatio() {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 100, SuccessThreshold: 1},
		breakerkit.WithFailureRatio(0.5, 10, time.Minute),
	)
	ctx := context.Background()
	errDown := errors.New("down")

	for i := range 10 {
		_ = cb.Execute(ctx, func(context.Context) error {
			if i%2 == 0 {
				return errDown
			}
			return nil
		})
	}
	fmt.Println(cb.State())

	// Output:
	// open
}

func ExampleWithOnStateChange() {
	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour},
		breakerkit.WithOnStateChange(func(from, to breakerkit.State) {
			fmt.Printf("transition: %s -> %s\n", from, to)
		}),
	)

	_ = cb.Execute(context.Background(), func(context.Context) error { return errors.New("boom") })

	// Output:
	// transition: closed -> open
}

func ExampleCircuitBreaker_Counts() {
	cb := breakerkit.New(breakerkit.Config{FailureThreshold: 3})
	ctx := context.Background()

	_ = cb.Execute(ctx, func(context.Context) error { return nil })
	_ = cb.Execute(ctx, func(context.Context) error { return errors.New("boom") })

	c := cb.Counts()
	fmt.Println(c.Requests, c.Successes, c.Failures, c.ConsecutiveFailures)

	// Output:
	// 2 1 1 1
}

func ExampleConfig_Validate() {
	err := breakerkit.Config{FailureThreshold: -1}.Validate()
	fmt.Println(errors.Is(err, breakerkit.ErrInvalidConfig))
	fmt.Println(err)

	// Output:
	// true
	// circuit_breaker.invalid_config: failure_threshold must not be negative, got -1
}
