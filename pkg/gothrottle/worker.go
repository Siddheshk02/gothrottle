// Package gothrottle provides a backend-agnostic distributed worker throttle.
package gothrottle

import (
	"context"
	"fmt"
	"sync"
)

// Task is a unit of work executed after both local and distributed capacity are
// available. The context passed to Task is canceled when the caller's context is
// canceled or when Worker shutdown begins.
type Task func(context.Context) error

// Worker coordinates local concurrency with distributed slot acquisition.
//
// A Worker is safe for concurrent use. It owns no database client directly;
// instead, all distributed coordination is delegated to the injected
// SlotManager. This keeps the package testable and lets applications provide
// their preferred backend.
type Worker struct {
	manager SlotManager
	cfg     config

	ctx    context.Context
	cancel context.CancelFunc

	sem    chan struct{}
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

// NewWorker creates a Worker that acquires distributed slots through manager.
func NewWorker(manager SlotManager, opts ...Option) (*Worker, error) {
	if manager == nil {
		return nil, fmt.Errorf("%w: slot manager is required", ErrInvalidConfig)
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{manager: manager, cfg: cfg, ctx: ctx, cancel: cancel, sem: make(chan struct{}, cfg.concurrency)}, nil
}

// Submit starts task asynchronously when local capacity is available and a
// distributed slot can be acquired. It returns ErrWorkerClosed if shutdown has
// begun. Task failures and slot release failures are passed to onDone.
func (w *Worker) Submit(ctx context.Context, task Task, onDone func(error)) error {
	if task == nil {
		return fmt.Errorf("%w: task is required", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return ErrWorkerClosed
	}
	w.wg.Add(1)
	w.mu.RUnlock()

	go w.run(ctx, task, onDone)
	return nil
}

// Do runs task synchronously under the worker throttle and returns the task or
// coordination error directly.
func (w *Worker) Do(ctx context.Context, task Task) error {
	done := make(chan error, 1)
	if err := w.Submit(ctx, task, func(err error) { done <- err }); err != nil {
		return err
	}
	return <-done
}

// Shutdown stops accepting new tasks, cancels worker-owned task contexts, and
// waits for in-flight tasks to finish or for ctx to be canceled.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		w.cancel()
	}
	w.mu.Unlock()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown worker: %w", ctx.Err())
	}
}

func (w *Worker) run(parent context.Context, task Task, onDone func(error)) {
	defer w.wg.Done()
	if onDone == nil {
		onDone = func(error) {}
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-w.ctx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	select {
	case w.sem <- struct{}{}:
		defer func() { <-w.sem }()
	case <-ctx.Done():
		onDone(fmt.Errorf("wait for local capacity: %w", ctx.Err()))
		return
	}

	acquireCtx, acquireCancel := context.WithTimeout(ctx, w.cfg.timeout)
	slot, err := w.manager.Acquire(acquireCtx, w.cfg.key)
	acquireCancel()
	if err != nil {
		onDone(fmt.Errorf("acquire distributed slot: %w", err))
		return
	}
	if slot == nil {
		onDone(fmt.Errorf("acquire distributed slot: %w", ErrSlotUnavailable))
		return
	}

	taskErr := task(ctx)
	releaseErr := slot.Release(context.Background())
	if taskErr != nil && releaseErr != nil {
		onDone(fmt.Errorf("task failed: %w; release slot: %v", taskErr, releaseErr))
		return
	}
	if taskErr != nil {
		onDone(fmt.Errorf("run task: %w", taskErr))
		return
	}
	if releaseErr != nil {
		onDone(fmt.Errorf("release slot: %w", releaseErr))
		return
	}
	onDone(nil)
}
