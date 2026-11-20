// Command http-client wraps HTTP calls to a flaky upstream with a circuit breaker.
//
// 5xx responses and transport errors are resource failures; 4xx responses mean the upstream
// is alive and are excluded with WithIsFailure.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/boostgo/breakerkit"
)

// statusError is returned for non-2xx responses.
type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("status %d", e.code) }

func main() {
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	cb := breakerkit.New(
		breakerkit.Config{FailureThreshold: 3, SuccessThreshold: 1, OpenTimeout: time.Minute},
		breakerkit.WithIsFailure(func(err error) bool {
			var se *statusError
			if errors.As(err, &se) {
				return se.code >= http.StatusInternalServerError
			}
			return true // transport errors
		}),
		breakerkit.WithOnStateChange(func(from, to breakerkit.State) {
			fmt.Printf("breaker: %s -> %s\n", from, to)
		}),
	)

	get := func(ctx context.Context, path string) error {
		return cb.Execute(ctx, func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+path, http.NoBody)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode >= http.StatusBadRequest {
				return &statusError{code: resp.StatusCode}
			}
			return nil
		})
	}

	ctx := context.Background()
	for range 5 {
		fmt.Println("GET /missing:", get(ctx, "/missing"))
	}
	for range 5 {
		fmt.Println("GET /orders:", get(ctx, "/orders"))
	}

	fmt.Printf("upstream hits: %d (the rest failed fast)\n", hits.Load())
	fmt.Printf("counts: %+v\n", cb.Counts())
}
