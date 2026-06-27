# gothrottle

`gothrottle` is a standalone Go library for coordinating task execution limits
across multiple application instances. It combines per-process concurrency with a
pluggable distributed slot backend, so teams can enforce limits such as “only 10
email deliveries across the whole fleet” while still writing ordinary Go task
functions.

## Status

This repository contains the core worker package, a mock-friendly backend
interface, an in-memory example backend, a runnable example, and unit tests.

## Project layout

```text
.
├── pkg/gothrottle        # Public library API
├── internal/memory       # In-process SlotManager used by examples/local demos
├── examples/basic        # Runnable example program
├── README.md             # Usage and design documentation
├── LICENSE
└── go.mod
```

## Why these patterns?

- **Standard layout (`pkg/`, `internal/`, `examples/`)** keeps the reusable API
  in `pkg/gothrottle`, private helpers in `internal`, and runnable sample code
  in `examples`.
- **Functional options** (`WithTimeout`, `WithConcurrency`, `WithKey`) keep
  construction readable and make it safe to add new configuration later without
  breaking callers.
- **Dependency injection** via `SlotManager` keeps the worker decoupled from
  Redis, Postgres, DynamoDB, or any other coordination system. Production code
  can inject a real backend while tests inject a small mock.
- **Context propagation** allows caller cancellation, worker shutdown, and slot
  acquisition timeouts to flow into both backend acquisition and user tasks.
- **`sync.WaitGroup` shutdown** lets the worker reject new submissions while it
  waits for in-flight goroutines to release slots cleanly.
- **Wrapped errors and sentinel errors** preserve implementation details while
  still allowing callers to use `errors.Is` for categories such as
  `ErrSlotUnavailable` or `ErrWorkerClosed`.

## Install

```bash
go get github.com/gothrottle/gothrottle
```

## Quick start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/gothrottle/gothrottle/pkg/gothrottle"
)

func main() {
    // Replace this with your Redis/Postgres/etc. implementation.
    var manager gothrottle.SlotManager = newRedisSlotManager()

    worker, err := gothrottle.NewWorker(
        manager,
        gothrottle.WithKey("email-delivery"),
        gothrottle.WithConcurrency(8),
        gothrottle.WithTimeout(2*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer worker.Shutdown(context.Background())

    err = worker.Do(context.Background(), func(ctx context.Context) error {
        return sendEmail(ctx)
    })
    if err != nil {
        log.Printf("task failed: %v", err)
    }
}
```

For a compileable local example, run:

```bash
go run ./examples/basic
```

## Public API overview

### Worker construction

```go
worker, err := gothrottle.NewWorker(
    manager,
    gothrottle.WithKey("payments"),
    gothrottle.WithConcurrency(4),
    gothrottle.WithTimeout(5*time.Second),
)
```

- `WithKey` selects the distributed capacity pool. Workers using the same key
  coordinate with each other through the injected `SlotManager`.
- `WithConcurrency` limits how many tasks this process runs at once.
- `WithTimeout` limits how long a task waits to acquire a distributed slot.

### Running work

Use `Do` when the caller should wait for completion:

```go
err := worker.Do(ctx, func(ctx context.Context) error {
    return processJob(ctx, job)
})
```

Use `Submit` when the caller wants asynchronous execution and callback-based
completion handling:

```go
err := worker.Submit(ctx, task, func(err error) {
    if err != nil {
        log.Printf("task failed: %v", err)
    }
})
```

### Shutdown

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := worker.Shutdown(shutdownCtx); err != nil {
    log.Printf("worker did not stop cleanly: %v", err)
}
```

`Shutdown` stops accepting new tasks, cancels worker-owned task contexts, and
waits for in-flight tasks to finish or for the shutdown context to expire.

## Implementing a backend

A backend only needs to implement two interfaces:

```go
type SlotManager interface {
    Acquire(ctx context.Context, key string) (Slot, error)
}

type Slot interface {
    Release(ctx context.Context) error
}
```

A Redis implementation might reserve a lease token with `SET NX PX`, store a
fencing value, or manage a sorted-set semaphore. A SQL implementation might use
advisory locks or a leases table. The worker does not care, as long as
`Acquire` respects `ctx` and every successful acquisition returns a `Slot` that
can be released.

## Error handling

The package exposes sentinel errors for common categories:

- `ErrSlotUnavailable` — distributed capacity could not be reserved.
- `ErrWorkerClosed` — work was submitted after shutdown began.
- `ErrInvalidConfig` — construction or options were invalid.

Errors are wrapped with `%w`, so callers can write:

```go
if errors.Is(err, gothrottle.ErrSlotUnavailable) {
    // retry later, enqueue elsewhere, or surface a backpressure response
}
```

## Testing

The worker is intentionally easy to test because it depends on an interface.
`pkg/gothrottle/worker_test.go` includes a mock `SlotManager` that uses a local
channel to prove the worker acquires and releases slots correctly under
concurrent submissions.

Run all tests with:

```bash
go test ./...
```

## License

GPL-3.0. See [LICENSE](LICENSE).
