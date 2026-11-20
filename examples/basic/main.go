// Command basic shows the Closed -> Open -> HalfOpen -> Closed cycle of a circuit breaker.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/boostgo/breakerkit"
)

func main() {
	cb := breakerkit.New(breakerkit.Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      200 * time.Millisecond,
	})
	ctx := context.Background()
	errDown := errors.New("resource unavailable")

	for i := 1; i <= 3; i++ {
		err := cb.Execute(ctx, func(context.Context) error { return errDown })
		fmt.Printf("call %d: err=%v state=%s\n", i, err, cb.State())
	}

	err := cb.Execute(ctx, func(context.Context) error { return nil })
	fmt.Printf("while open: err=%v\n", err)

	time.Sleep(250 * time.Millisecond)

	for i := 1; i <= 2; i++ {
		err := cb.Execute(ctx, func(context.Context) error { return nil })
		fmt.Printf("probe %d: err=%v state=%s\n", i, err, cb.State())
	}

	fmt.Printf("counts: %+v\n", cb.Counts())
}
