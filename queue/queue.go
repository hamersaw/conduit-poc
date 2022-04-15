package queue

import (
	"context"
	"fmt"
	"time"
)

type Queue struct {
	topic string
}

func NewQueue(topic string) Queue {
	return Queue{
		topic: topic,
	}
}

func (q Queue) Start(ctx context.Context) error {
	// start refresh routine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			fmt.Println("queue refresh")

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
