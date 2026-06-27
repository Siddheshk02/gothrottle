package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Siddheshk02/gothrottle/internal/memory"
	"github.com/Siddheshk02/gothrottle/pkg/gothrottle"
)

func main() {
	manager, err := memory.NewManager(2)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := gothrottle.NewWorker(
		manager,
		gothrottle.WithKey("email-delivery"),
		gothrottle.WithConcurrency(4),
		gothrottle.WithTimeout(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	for i := 0; i < 5; i++ {
		i := i
		if err := worker.Submit(context.Background(), func(ctx context.Context) error {
			fmt.Printf("running task %d\n", i)
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(err error) {
			if err != nil {
				log.Printf("task failed: %v", err)
			}
		}); err != nil {
			log.Fatal(err)
		}
	}
}
