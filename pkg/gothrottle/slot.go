package gothrottle

import "context"

// Slot represents a distributed execution lease acquired from a SlotManager.
//
// Implementations typically hold backend metadata such as a Redis key, token,
// or fencing value. Release must be safe to call exactly once after the task
// finishes; the Worker guarantees that Release is called for every successful
// acquisition.
type Slot interface {
	// Release returns the execution lease to the backing SlotManager.
	Release(ctx context.Context) error
}

// SlotManager coordinates distributed task capacity across worker processes.
//
// Acquire should block until a slot is available, the context is canceled, or
// the implementation determines that capacity cannot be reserved. Backend
// packages can implement this interface for Redis, Postgres advisory locks,
// DynamoDB leases, or any other coordination primitive without changing Worker.
type SlotManager interface {
	// Acquire reserves one execution slot for key and returns a lease that must
	// be released after task completion.
	Acquire(ctx context.Context, key string) (Slot, error)
}
