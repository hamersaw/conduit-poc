package conduit

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Queue struct {
	buffer                 chan *Task
	bufferLeaseExpirations sync.Map
	bufferSize             int32
	maxBufferSize          int
	topic                  string
}

func NewQueue(bufferSize int, topic string) Queue {
	return Queue{
		buffer:        make(chan *Task, bufferSize),
		bufferSize:    int32(0),
		maxBufferSize: bufferSize,
		topic:         topic,
	}
}

func (q *Queue) GetTask(ctx context.Context) (*Task, error) {
	select {
	case task := <- q.buffer:
		q.bufferLeaseExpirations.Delete(task.ID)
		atomic.AddInt32(&q.bufferSize, -1)
		return task, nil
	case <- ctx.Done():
		return nil, fmt.Errorf("context canceled request")
	}
}

// TODO - should we add a lock here? everytime we insert we check if the buffer size < maxBufferSize
// and then attempt to add a task - because chan's are always bounded, in the case of multiple adds
// this function call could block causing either a task insert or the buffer refresh to block
// ... seems kind of messy
func (q *Queue) AddTask(ctx context.Context, task *Task) error {
	q.buffer <- task
	q.bufferLeaseExpirations.Store(task.ID, task.LeaseExpirationTs)
	atomic.AddInt32(&q.bufferSize, 1)

	return nil
}

func (q *Queue) GetRemainingBufferSize() int {
	return q.maxBufferSize - int(atomic.LoadInt32(&q.bufferSize))
}

func (q *Queue) Start(ctx context.Context, db *sql.DB) error {
	// start buffer refresh routine
	go func() {
		ticker := time.NewTicker(5 * time.Second) // TODO - parameterize
		for {
			// TODO update existing buffer lease
			// TODO - if this fails longer than > lease_expiration_duration then drain buffer
			// we can store the lease expiration ts in the id lookup set and update here
			log.Printf("TODO - update existing buffer lease")

			// get tasks from db to fill out buffer
			remainingBufferSize := q.GetRemainingBufferSize()
			if remainingBufferSize > 0 {
				tasks, err := GetBufferTasks(ctx, db, q.topic, q.GetRemainingBufferSize())
				if err != nil {
					log.Printf("failed to retrieve buffered tasks", err)
				}

				// add tasks to buffer
				count := 0
				for _, task := range tasks {
					q.AddTask(ctx, task)
				}

				if count > 0 {
					log.Printf("added %d tasks to buffer", count)
				}
			}

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
