package conduit

import (
	"context"
	"log"
	"time"

	protos "github.com/hamersaw/conduit-poc/protos/gen/pb-go"
)

type Queue struct {
	topic string
}

func NewQueue(topic string) Queue {
	return Queue{
		topic: topic,
	}
}

func (q *Queue) AddTask(ctx context.Context, task *protos.Task) error {
	// TODO - add to DB

	// TODO - check for pending long polls

	// TODO - add to buffer (if space)

	return nil
}

func (q *Queue) Start(ctx context.Context) error {
	// start buffer refresh routine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			log.Printf("TODO - refresh queue %s", q.topic)

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
