package conduit

import (
	"context"
	"database/sql"
	"fmt"
	//"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ingressRequest struct {
	task *Task
	responseChan chan <-*ingressResponse
}

type ingressResponse struct {
	err error
}

type Ingress struct {
	db      *sql.DB
	queues  *sync.Map
	requests chan *ingressRequest
}

func NewIngress(db *sql.DB, queues *sync.Map, bufferSize int) Ingress {
	return Ingress{
		db:       db,
		queues:   queues,
		requests: make(chan *ingressRequest, bufferSize),
	}
}

func (i *Ingress) Start(ctx context.Context) error {
	// start ingress routine
	go func() {
		for {
			select {
			case request := <-i.requests:
				err := i.process(ctx, request.task)
				response := ingressResponse{
					err: err,
				}

				request.responseChan <- &response
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (i *Ingress) AddTask(ctx context.Context, task Task) error {
	responseChan := make(chan *ingressResponse)
	request := &ingressRequest{
		task:         &task,
		responseChan: responseChan,
	}

	select {
	case i.requests <- request:
		select {
		// TODO - handle shutdown?
		case response := <- responseChan:
			return response.err
		}
	default:
		return fmt.Errorf("requests channel is full")
	}
}

func (i *Ingress) process(ctx context.Context, task *Task) error {
	// get queue for topic
	o, ok := i.queues.Load(task.Topic)
	if !ok {
		return status.Errorf(codes.NotFound, fmt.Sprintf("queue for topic '%s' does not exist", task.Topic))
	}
	q, _ := o.(*Queue)

	persistOkChan := make(chan bool)
	taskOffer := &TaskOffer{
		persistOkChan: persistOkChan,
		task:          task,
	}

	var err error

	select {
	case q.head <- taskOffer:
		// attempt to pass through directly to the queue head
		// TODO - document
		result := make(chan error)
		go func() {
			task.LeaseExpirationTs = time.Now().Add(time.Second * 40) // TODO - parameterize the lease duration

			// add task to DB
			err := CreateTask(i.db, task)
			if err != nil {
				persistOkChan <- false
				result <- status.Errorf(codes.Internal, fmt.Sprintf("failed to write task '%v' to db with err: %v", *task, err))
			}

			// TODO add to ack manager

			persistOkChan <- true
			result <- nil
		}()

		err = <- result
	default:
		// add task to DB
		if e := CreateTask(i.db, task); e != nil {
			err = status.Errorf(codes.Internal, fmt.Sprintf("failed to write task '%v' to db with err: %v", *task, e))
		}
	}

	return err

	// might need to rework - two things to note
	// (1) use this for automatic passthrough to long-polling clients - otherwise we need to wait for the auto buffer refresh
	// (2) without the use of a lock we may have to wait if the auto refresh races with this - this is highly unlikely
	/*bufferPassThrough := false
	if q.GetRemainingBufferSize() > 0 {
		bufferPassThrough = true
		task.LeaseExpirationTs = time.Now().Add(time.Second * 40) // TODO - parameterize the lease duration
	}

	log.Printf("bufferPassThrough:%v remainingBufferSize:%d", bufferPassThrough, q.GetRemainingBufferSize())

	// add task to DB
	if err := CreateTask(i.db, task); err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("failed to write task '%v' to db with err: %v", *task, err))
	}

	log.Printf("wrote task to db")

	if bufferPassThrough {
		// add task to queue
		if err := q.AddTask(ctx, task); err != nil {
			return status.Errorf(codes.Internal, fmt.Sprintf("failed to add task '%v' with err: %v", *task, err))
		}
	}*/

	return nil
}
