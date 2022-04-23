package conduit

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/uptrace/bun"
)

type TaskOffer struct {
	persistOkChan chan bool
	task          *Task
}

type Queue struct {
	buffer               chan *Task
	bufferSize           int
	db                   *bun.DB
	head                 chan *TaskOffer
	leasedTaskIds        *Set
	leaseDuration        time.Duration
	leaseUpdateInterval  time.Duration
	topic                string
	queueRefreshInterval time.Duration
}

func NewQueue(bufferSize int, db *bun.DB, leaseDuration, leaseUpdateInterval time.Duration, topic string, queueRefreshInterval time.Duration) Queue {
	return Queue{
		db:                   db,
		buffer:               make(chan *Task, bufferSize-1),
		bufferSize:           bufferSize,
		head:                 make(chan *TaskOffer),
		leasedTaskIds:        new(Set),
		leaseDuration:        leaseDuration,
		leaseUpdateInterval:  leaseUpdateInterval,
		topic:                topic,
		queueRefreshInterval: queueRefreshInterval,
	}
}

func (q *Queue) GetTask(ctx context.Context) (*Task, error) {
	for {
		select {
		case taskOffer := <- q.head:
			// ensure task has been persisted
			defer close(taskOffer.persistOkChan)
			if ok := <- taskOffer.persistOkChan; !ok {
				continue
			}

			// TODO - set started_at and heartbeat_expiration_at

			// remove leased task id
			q.leasedTaskIds.Delete(taskOffer.task.ID)
			return taskOffer.task, nil
		case <- ctx.Done():
			return nil, fmt.Errorf("context canceled request")
		}
	}
}

func (q *Queue) Start(ctx context.Context) error {
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

	// start lease update routine
	go func() {
		ticker := time.NewTicker(q.leaseUpdateInterval)
		for {
			// TODO - if this fails longer than > lease_expiration_duration then drain buffer

			// collect ids from leasedTaskIds map
			var ids []string
			q.leasedTaskIds.Range(func(key, _ interface{}) bool {
				if id, ok := key.(string); ok {
					ids = append(ids, id)
				}
				return true
			})

			// update lease expirations
			if len(ids) > 0 {
				if err := LeaseTasks(ctx, q.db, ids, q.leaseDuration); err != nil {
					log.Printf("failed to update lease expirations with err: %v", err)
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// start buffer refresh routine
	go func() {
		ticker := time.NewTicker(q.queueRefreshInterval)
		for {
			// get tasks from db to fill out buffer
			remainingBufferSize := q.bufferSize - len(q.buffer)
			if remainingBufferSize > 0 {
				tasks, err := GetBufferTasks(ctx, q.db, q.topic, q.leaseDuration, remainingBufferSize)
				if err != nil {
					log.Printf("failed to retrieve buffered tasks with err: %v", err)
				}

				// add tasks to buffer
				for _, task := range tasks {
					q.buffer <- task
					q.leasedTaskIds.Add(task.ID)
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
