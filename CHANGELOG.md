# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-11-20

First public release.

### Added

- `CircuitBreaker` with `New(Config, ...Option)` and `Execute(ctx, fn)`: a goroutine-safe
  Closed / Open / HalfOpen state machine with no dependencies and no background goroutines.
- `Config` (`FailureThreshold`, `SuccessThreshold`, `OpenTimeout`, `HalfOpenMaxProbes`) with
  `json`/`yaml` tags, defaults (`DefaultFailureThreshold` = 5, `DefaultSuccessThreshold` = 2,
  `DefaultOpenTimeout` = 30s) and `Config.Validate` / `ErrInvalidConfig`.
- `HalfOpenMaxProbes`: limits concurrent probe calls in HalfOpen (one by default).
- Stale outcome protection: a result that returns after the state has changed is ignored.
- `WithIsFailure`: a predicate that separates business errors from resource failures.
- Caller cancellation is neutral: `context.Canceled` / `context.DeadlineExceeded` with a
  cancelled caller context does not change counters or state.
- A panic in `fn` counts as a failure, releases the probe slot and is re-raised.
- `WithFailureRatio(ratio, minRequests, window)`: failure-ratio trigger over a 10-bucket sliding
  window, in addition to "N failures in a row".
- `WithOnStateChange`: a callback invoked exactly once per transition, outside the mutex.
- `CircuitBreaker.State`, `CircuitBreaker.Counts` and the `Counts` snapshot (with the monotonic
  `Rejected` counter) for metrics.
- `State` type with `StateClosed`, `StateOpen`, `StateHalfOpen` and `String`/`Is`/`Empty`.
- `ErrCircuitOpen`.
- README, godoc examples, runnable programs under `examples/`, CI, Makefile, golangci-lint config.

