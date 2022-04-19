package conduit

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	//"sync/atomic"
	"time"
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

/*func (q *Queue) GetTask(ctx context.Context) (*Task, error) {
	select {
	case task := <- q.buffer:
		q.bufferLeaseExpirations.Delete(task.ID)
		atomic.AddInt32(&q.bufferSize, -1)
		return task, nil
	case <- ctx.Done():
		return nil, fmt.Errorf("context canceled request")
	}
}*/

// TODO - should we add a lock here? everytime we insert we check if the buffer size < maxBufferSize
// and then attempt to add a task - because chan's are always bounded, in the case of multiple adds
// this function call could block causing either a task insert or the buffer refresh to block
// ... seems kind of messy
/*func (q *Queue) AddTask(ctx context.Context, task *Task) error {
	atomic.AddInt32(&q.bufferSize, 1)
	q.bufferLeaseExpirations.Store(task.ID, task.LeaseExpirationTs)
	q.buffer <- task

	return nil
}*/

/*func (q *Queue) GetRemainingBufferSize() int {
	return q.maxBufferSize - int(atomic.LoadInt32(&q.bufferSize))
}*/

func (q *Queue) Start(ctx context.Context, db *sql.DB) error {
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
				count := 0
				for _, task := range tasks {
					q.buffer <- task
					// TODO - add to lease extension routine
					//q.AddTask(ctx, task)
				}

				if count > 0 {
					log.Printf("added %d tasks to buffer", count)
				}
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
