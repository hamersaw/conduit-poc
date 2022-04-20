package conduit

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

type TaskOffer struct {
	persistOkChan chan bool
	task          *Task
}

type Queue struct {
	buffer                 chan *Task
	bufferLeaseExpirations sync.Map
	bufferSize             int
	head                   chan *TaskOffer
	topic                  string
}

func NewQueue(bufferSize int, topic string) Queue {
	return Queue{
		buffer:     make(chan *Task, bufferSize-1),
		bufferSize: bufferSize,
		head:       make(chan *TaskOffer),
		topic:      topic,
	}
}

func (q *Queue) GetTask(ctx context.Context) (*Task, error) {
	for {
		select {
		case taskOffer := <- q.head:
			defer close(taskOffer.persistOkChan) // TODO - make sure we close the other channels
			if ok := <- taskOffer.persistOkChan; !ok {
				continue
			}
			// TODO remove from ack manager
			return taskOffer.task, nil
		case <- ctx.Done():
			return nil, fmt.Errorf("context canceled request")
		}
	}
}

func (q *Queue) Start(ctx context.Context, db *bun.DB) error {
	// start buffer dispatch routine
	go func() {
		for {
			task := <- q.buffer

			persistOkChan := make(chan bool)
			q.head <- &TaskOffer{
				task:          task,
				persistOkChan: persistOkChan,
			}

			persistOkChan <- true
		}
	}()

	// start buffer refresh routine
	go func() {
		ticker := time.NewTicker(5 * time.Second) // TODO - parameterize
		for {
			// TODO update existing buffer lease
			// TODO - if this fails longer than > lease_expiration_duration then drain buffer
			// we can store the lease expiration ts in the id lookup set and update here
			log.Printf("TODO - update existing buffer lease")

			// get tasks from db to fill out buffer
			//remainingBufferSize := q.GetRemainingBufferSize()
			remainingBufferSize := q.bufferSize - len(q.buffer)
			if remainingBufferSize > 0 {
				tasks, err := GetBufferTasks(ctx, db, q.topic, remainingBufferSize)
				if err != nil {
					log.Printf("failed to retrieve buffered tasks", err)
				}

				// add tasks to buffer
				for _, task := range tasks {
					q.buffer <- task
				}

				log.Printf("added %d tasks to buffer", len(tasks))
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return nil
}
