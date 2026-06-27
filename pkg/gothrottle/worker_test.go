package gothrottle_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Siddheshk02/gothrottle/pkg/gothrottle"
)

func TestWorkerUsesInjectedSlotManager(t *testing.T) {
	manager := newMockSlotManager(1)
	worker, err := gothrottle.NewWorker(manager, gothrottle.WithConcurrency(4), gothrottle.WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	var running int32
	var maxRunning int32
	done := make(chan error, 8)

	for i := 0; i < 8; i++ {
		if err := worker.Submit(context.Background(), func(ctx context.Context) error {
			current := atomic.AddInt32(&running, 1)
			defer atomic.AddInt32(&running, -1)
			for {
				max := atomic.LoadInt32(&maxRunning)
				if current <= max || atomic.CompareAndSwapInt32(&maxRunning, max, current) {
					break
				}
			}

			select {
			case <-time.After(10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(err error) { done <- err }); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatalf("task error = %v", err)
		}
	}

	if got := atomic.LoadInt32(&maxRunning); got != 1 {
		t.Fatalf("max concurrent tasks = %d, want 1", got)
	}
	if manager.acquired() != 8 || manager.released() != 8 {
		t.Fatalf("acquired/released = %d/%d, want 8/8", manager.acquired(), manager.released())
	}
}

type mockSlotManager struct {
	sem      chan struct{}
	acquires atomic.Int32
	releases atomic.Int32
}

func newMockSlotManager(limit int) *mockSlotManager {
	return &mockSlotManager{sem: make(chan struct{}, limit)}
}

func (m *mockSlotManager) Acquire(ctx context.Context, key string) (gothrottle.Slot, error) {
	select {
	case m.sem <- struct{}{}:
		m.acquires.Add(1)
		return &mockSlot{release: func() { <-m.sem; m.releases.Add(1) }}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockSlotManager) acquired() int32 { return m.acquires.Load() }
func (m *mockSlotManager) released() int32 { return m.releases.Load() }

type mockSlot struct {
	once    sync.Once
	release func()
}

func (s *mockSlot) Release(context.Context) error {
	s.once.Do(s.release)
	return nil
}
