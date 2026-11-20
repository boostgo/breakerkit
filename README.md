# breakerkit

[![Go Reference](https://pkg.go.dev/badge/github.com/boostgo/breakerkit.svg)](https://pkg.go.dev/github.com/boostgo/breakerkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/boostgo/breakerkit)](https://goreportcard.com/report/github.com/boostgo/breakerkit)
[![CI](https://github.com/boostgo/breakerkit/actions/workflows/ci.yml/badge.svg)](https://github.com/boostgo/breakerkit/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A small, dependency-free [circuit breaker](https://martinfowler.com/bliki/CircuitBreaker.html)
for Go that is built for real concurrent services: it limits half-open probes instead of letting
the whole worker pool hammer a recovering resource, ignores stale results and caller
cancellations, tells business errors from resource failures, opens on a sliding-window failure
ratio, and fires exactly one callback per state transition no matter how many goroutines see it.
No background goroutines, no tickers, just a mutex.

## Features

- Classic **Closed → Open → HalfOpen → Closed** state machine, safe for any number of goroutines.
- **Half-open probe limit** (`HalfOpenMaxProbes`): only N calls test a recovering resource.
- **Stale outcome protection**: a call that returns after the state changed does not affect it.
- **Business vs resource errors** (`WithIsFailure`): a 4xx is not an outage.
- **Cancellation-aware**: a cancelled caller context does not count as a failure.
- **Panic-safe**: a panic in the protected call counts as a failure, frees the probe slot and is re-raised.
- **Failure ratio** (`WithFailureRatio`): open on "≥ 50% of the last minute", not only "N in a row".
- **Exactly-once transition callback** (`WithOnStateChange`), called outside the lock.
- **Metrics snapshot** (`Counts`) including a monotonic `Rejected` counter.
- Config struct with `json`/`yaml` tags, sane defaults and `Validate()`.
- Zero dependencies outside the standard library.

## Install

```sh
go get github.com/boostgo/breakerkit
```

Requires Go 1.23 or newer.

## Quick start

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/boostgo/breakerkit"
)

var cb = breakerkit.New(breakerkit.Config{
	FailureThreshold: 5,                // open after 5 failures in a row
	SuccessThreshold: 2,                // close after 2 successful probes
	OpenTimeout:      30 * time.Second, // wait before probing
})

func fetch(ctx context.Context, id string) (Data, error) {
	var out Data
	err := cb.Execute(ctx, func(ctx context.Context) error {
		var err error
		out, err = api.Get(ctx, id)
		return err
	})
	if errors.Is(err, breakerkit.ErrCircuitOpen) {
		return Data{}, ErrServiceUnavailable // fail fast or serve a fallback
	}
	return out, err
}
```

`Execute` returns the error of your function as is, or `ErrCircuitOpen` if the call was not
admitted (your function is not invoked then).

## How it works

```
        N failures in a row          OpenTimeout elapsed
Closed ─────────────────────► Open ─────────────────────► HalfOpen
  ▲                                                          │
  │               M successes in a row                       │
  └──────────────────────────────────────────────────────────┤
                                                             │ any failure
                                                             ▼
                                                            Open
```

| State       | Behaviour                                                                                   |
|-------------|---------------------------------------------------------------------------------------------|
| `closed`    | Calls pass. Failures are counted; the threshold (or the ratio) opens the circuit.           |
| `open`      | Every call is rejected immediately with `ErrCircuitOpen`. After `OpenTimeout` → half-open.   |
| `half_open` | Up to `HalfOpenMaxProbes` concurrent probes pass. M successes → closed, any failure → open. |

## Examples

### Thresholds and timeouts

```go
cb := breakerkit.New(breakerkit.Config{
	FailureThreshold:  5,
	SuccessThreshold:  2,
	OpenTimeout:       30 * time.Second,
	HalfOpenMaxProbes: 1,
})
```

The failure counter in `closed` counts failures **in a row**: any success resets it. Zero values
mean "use the default"; negative values are also replaced by `New`, but `Validate` reports them:

```go
if err := cfg.Validate(); err != nil { // errors.Is(err, breakerkit.ErrInvalidConfig)
	log.Fatal(err)
}
```

`Config` has `json` and `yaml` tags, so it can live in your service config:

```yaml
paymentsBreaker:
  failureThreshold: 10
  successThreshold: 3
  openTimeout: 30s
  halfOpenMaxProbes: 2
```

### Business errors are not failures: `WithIsFailure`

By default every error is a failure. If the resource answered but the answer is a business-level
error, return `false` from the predicate: the outcome then counts as a **success** (it resets the
"in a row" counter and counts toward a half-open probe). `Execute` still returns the error.

```go
cb := breakerkit.New(cfg, breakerkit.WithIsFailure(func(err error) bool {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500 // 4xx means the payments API is alive
	}
	return true // transport errors are failures
}))
```

For gRPC the predicate typically checks `status.Code(err) == codes.Unavailable` (the breaker does
not import gRPC; this is your code).

### Context cancellation

If the caller's context is done (`ctx.Err() != nil`) and the function returned
`context.Canceled` / `context.DeadlineExceeded`, the outcome is **ignored**: counters and state do
not change and the half-open probe slot is released. Shutdowns and consumer-group rebalances do
not open the circuit. A `DeadlineExceeded` from the function's *own* timeout while the caller's
context is alive is a failure.

### Panics

A panic in the protected function counts as a failure, the probe slot is released and the panic is
re-raised. The breaker never gets stuck in `half_open`.

### Failure ratio: `WithFailureRatio`

```go
// open when at least 20 calls happened in the last minute and >= 50% of them failed
cb := breakerkit.New(cfg, breakerkit.WithFailureRatio(0.5, 20, time.Minute))
```

The "N in a row" trigger keeps working; whichever fires first opens the circuit. The window slides
over 10 buckets and is advanced by the breaker clock on calls (no goroutines), so the window edge
precision is `window/10`. Invalid arguments (`ratio` outside `(0, 1]`, `minRequests < 1`,
`window <= 0`) leave the mode disabled.

### Metrics: `State` and `Counts`

```go
state := cb.State()  // breakerkit.StateClosed / StateOpen / StateHalfOpen
c := cb.Counts()     // Requests, Successes, Failures, ConsecutiveSuccesses, ConsecutiveFailures, Rejected

gauge.Set(float64(stateValue(state)))
rejected.Set(float64(c.Rejected))
```

Counters are reset on every transition (with `WithFailureRatio`, `Requests`/`Successes`/`Failures`
are the window totals). `Rejected` is monotonic over the breaker lifetime.

### Alerting on transitions: `WithOnStateChange`

```go
cb := breakerkit.New(cfg, breakerkit.WithOnStateChange(func(from, to breakerkit.State) {
	if to == breakerkit.StateOpen {
		alerts <- "payments API circuit opened" // hand over, do not block
	}
}))
```

The callback fires **exactly once per actual transition**, regardless of how many goroutines
observe it. It runs synchronously but outside the internal mutex, so it may call `cb.State()`.
Keep it fast. Delivery order under heavy concurrency is best-effort; "exactly once per
transition" is guaranteed, global ordering is not.

Do **not** detect transitions by comparing `State()` before and after `Execute` in concurrent
code: every goroutine inside `Execute` at that moment sees the same transition (N duplicate
alerts), and an `open → half_open → open` round trip between two reads is missed entirely.

More runnable programs live in [`examples/`](examples):
[`basic`](examples/basic/main.go), [`http-client`](examples/http-client/main.go),
[`state-change`](examples/state-change/main.go).

## Configuration reference

### `Config`

| Field               | Type            | Default | YAML / JSON key                             | Description                                     |
|---------------------|-----------------|---------|---------------------------------------------|-------------------------------------------------|
| `FailureThreshold`  | `int`           | `5`     | `failureThreshold` / `failure_threshold`    | Consecutive failures that open the circuit      |
| `SuccessThreshold`  | `int`           | `2`     | `successThreshold` / `success_threshold`    | Consecutive half-open successes that close it   |
| `OpenTimeout`       | `time.Duration` | `30s`   | `openTimeout` / `open_timeout`              | Time in `open` before moving to `half_open`     |
| `HalfOpenMaxProbes` | `int`           | `1`     | `halfOpenMaxProbes` / `half_open_max_probes`| Concurrent probes admitted in `half_open`       |

### Options

| Option                                       | Description                                                    |
|----------------------------------------------|----------------------------------------------------------------|
| `WithIsFailure(func(error) bool)`            | Decide which errors are resource failures (default: all)      |
| `WithFailureRatio(ratio, minRequests, window)` | Additional sliding-window failure-ratio trigger              |
| `WithOnStateChange(func(from, to State))`    | Exactly-once callback per state transition                     |

### Errors

| Error              | Value                             | Returned by                         |
|--------------------|-----------------------------------|-------------------------------------|
| `ErrCircuitOpen`   | `circuit_breaker.circuit_open`    | `Execute` when the call is rejected |
| `ErrInvalidConfig` | `circuit_breaker.invalid_config`  | `Config.Validate`                   |

## FAQ

**One breaker per what?** One per protected resource (an upstream API, a database). Sharing one
breaker between unrelated resources lets a failure of one block the others.

**Does the half-open probe limit affect sequential code?** No. A probe slot is released when the
call returns, so a single goroutine calling one after another is never rejected by the limit.

**What happens to a slow call that started before the circuit opened?** Its result is ignored:
the breaker tracks a state generation and drops outcomes from older generations, so a stale error
from the outage cannot immediately re-open a freshly recovered circuit.

**Does it start goroutines or timers?** No. Time is checked on calls only.

## Contributing

Issues and pull requests are welcome. Before submitting, please run:

```sh
make check   # go vet, golangci-lint, go test -race
```

Add tests for behaviour changes and keep the public API documented.

## License

[MIT](LICENSE) © 2026 boostgo
