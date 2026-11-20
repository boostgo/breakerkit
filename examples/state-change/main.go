// Command state-change shows that WithOnStateChange fires exactly once per transition even
// when many goroutines hit the failure at the same time, which makes it the right place for
// alerting.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/boostgo/breakerkit"
)

func main() {
	alerts := make(chan string, 16)

	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour},
		breakerkit.WithOnStateChange(func(from, to breakerkit.State) {
			// keep the callback fast: hand the alert over instead of sending it inline
			alerts <- fmt.Sprintf("%s -> %s", from, to)
		}),
	)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = cb.Execute(context.Background(), func(context.Context) error {
				return errors.New("boom")
			})
		}()
	}
	close(start)
	wg.Wait()
	close(alerts)

	n := 0
	for a := range alerts {
		n++
		fmt.Println("alert:", a)
	}
	fmt.Printf("32 goroutines, %d alert(s), rejected=%d\n", n, cb.Counts().Rejected)
}
