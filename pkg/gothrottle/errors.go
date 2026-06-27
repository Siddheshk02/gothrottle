package gothrottle

import "errors"

var (
	// ErrSlotUnavailable indicates that the configured SlotManager could not
	// reserve capacity for a task before the worker timeout or context deadline.
	ErrSlotUnavailable = errors.New("gothrottle: slot unavailable")

	// ErrWorkerClosed indicates that a task was submitted after shutdown began.
	ErrWorkerClosed = errors.New("gothrottle: worker closed")

	// ErrInvalidConfig indicates that worker construction received invalid options.
	ErrInvalidConfig = errors.New("gothrottle: invalid config")
)
