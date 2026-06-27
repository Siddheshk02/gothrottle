// Package memory contains an in-process SlotManager useful for examples and
// local development. Production distributed deployments should use a backend
// shared by all application instances, such as Redis.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/gothrottle/gothrottle/pkg/gothrottle"
)

// Manager is an in-memory SlotManager keyed by throttle name.
type Manager struct {
	limit int

	mu    sync.Mutex
	slots map[string]chan struct{}
}

// NewManager creates a Manager with limit slots per key.
func NewManager(limit int) (*Manager, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("memory manager limit must be positive")
	}
	return &Manager{limit: limit, slots: make(map[string]chan struct{})}, nil
}

// Acquire reserves one in-memory slot for key.
func (m *Manager) Acquire(ctx context.Context, key string) (gothrottle.Slot, error) {
	ch := m.channel(key)
	select {
	case ch <- struct{}{}:
		return &slot{release: func() { <-ch }}, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", gothrottle.ErrSlotUnavailable, ctx.Err())
	}
}

func (m *Manager) channel(key string) chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.slots[key]; ok {
		return ch
	}
	ch := make(chan struct{}, m.limit)
	m.slots[key] = ch
	return ch
}

type slot struct {
	once    sync.Once
	release func()
}

func (s *slot) Release(context.Context) error {
	s.once.Do(s.release)
	return nil
}
