package gothrottle

import (
	"fmt"
	"time"
)

const (
	defaultKey         = "gothrottle:default"
	defaultTimeout     = 30 * time.Second
	defaultConcurrency = 1
)

type config struct {
	key         string
	timeout     time.Duration
	concurrency int
}

// Option configures a Worker during construction.
type Option func(*config) error

// WithKey sets the logical distributed throttle key used for slot acquisition.
// Workers that share a key share the same distributed capacity pool.
func WithKey(key string) Option {
	return func(c *config) error {
		if key == "" {
			return fmt.Errorf("%w: key cannot be empty", ErrInvalidConfig)
		}
		c.key = key
		return nil
	}
}

// WithTimeout sets the maximum time a Worker waits to acquire a distributed
// slot for each task, unless the caller's context is canceled first.
func WithTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: timeout must be positive", ErrInvalidConfig)
		}
		c.timeout = timeout
		return nil
	}
}

// WithConcurrency sets the maximum number of tasks this Worker instance runs
// concurrently. Distributed limits are still enforced by the SlotManager.
func WithConcurrency(concurrency int) Option {
	return func(c *config) error {
		if concurrency <= 0 {
			return fmt.Errorf("%w: concurrency must be positive", ErrInvalidConfig)
		}
		c.concurrency = concurrency
		return nil
	}
}

func defaultConfig() config {
	return config{key: defaultKey, timeout: defaultTimeout, concurrency: defaultConcurrency}
}
