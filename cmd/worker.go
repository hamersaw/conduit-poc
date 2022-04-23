package main

import (
	"context"
	"fmt"
	"flag"
	"log"
	"sync"
	"time"

	proto "github.com/hamersaw/conduit-poc/protos/gen/pb-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	host = flag.String("grpc-host", "127.0.0.1", "The grpc server host")
	port = flag.Int("grpc-port", 50051, "The grpc server port")
	topic = flag.String("topic", "foo", "The queue topic for this worker")
	workerCount = flag.Int("worker-count", 1, "The number of workers to simulate")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	waitGroup := new(sync.WaitGroup)
	waitGroup.Add(*workerCount)
	for i := 0; i<*workerCount; i++ {
		worker := Worker{}
		worker.Start(ctx, waitGroup)
	}

	waitGroup.Wait()
}

type Worker struct {
}

func (w *Worker) Start(ctx context.Context, waitGroup *sync.WaitGroup) error {
	// connect to grpc endpoint
	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC endpoint '%s:%d' with err: %v", *host, *port, err)
	}
	//defer conn.Close()

	// create TaskServiceClient
	client := proto.NewTaskServiceClient(conn)
	request := &proto.GetTaskRequest{
		Topic: *topic,
	}

	completedChan := make(chan string)
	inProgressChan := make(chan string)

	// start heartbeat routine
	go func() {
		ticker := time.NewTicker(time.Second * 10) // TODO parameterize
		var completedIds, inProgressIds []string

		for {
			select {
			case completedId := <- completedChan:
				// remove id from inProgressIds
				index := -1
				for i, id := range inProgressIds {
					if id == completedId {
						index = i 
						break
					}
				}

				inProgressIds = append(inProgressIds[:index], inProgressIds[index+1:]...)
				completedIds = append(completedIds, completedId)

				// send a heartbeat request to signify the completed task
				request := &proto.HeartbeatRequest{
					CompletedIds: completedIds,
				}

				timeoutCtx, cancel := context.WithTimeout(ctx, time.Second * 2) // TODO - parameterize
				defer cancel()

				_, err := client.Heartbeat(timeoutCtx, request)
				if err != nil {
					log.Printf("failed heartbeat with err: %v", err)
					continue
				}

				completedIds = completedIds[:0]
			case inProgressId := <- inProgressChan:
				inProgressIds = append(inProgressIds, inProgressId)
			case <- ticker.C:
				if len(completedIds) != 0 || len(inProgressIds) != 0 {
					// send a heartbeat request
					request := &proto.HeartbeatRequest{
						CompletedIds:  completedIds,
						InProgressIds: inProgressIds,
					}

					timeoutCtx, cancel := context.WithTimeout(ctx, time.Second * 2) // TODO - parameterize
					defer cancel()

					_, err := client.Heartbeat(timeoutCtx, request)
					if err != nil {
						log.Printf("failed heartbeat with err: %v", err)
						continue
					}

					completedIds = completedIds[:0]
				}
			}
		}
	}()

	// start worker routine
	go func() {
		for {
			// send AddTaskRequest
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Second * 10) // TODO - parameterize long poll duration
			defer cancel()

			response, err := client.GetTask(timeoutCtx, request)
			if err != nil {
				if status.Code(err) == codes.Canceled {
					log.Printf("grpc connection canceled")
					time.Sleep(time.Second * 1)
				} else {
					log.Printf("failed to retrieve task: %v", err)
				}
				continue
			}

			task := response.GetTask()
			inProgressChan <- task.Id
			//log.Printf("received task '%v'", *task)
			time.Sleep(task.GetExecutionDuration().AsDuration())
			completedChan <- task.Id
			log.Printf("completed task '%v'", *task)

			// TODO - send completion?
		}
	}()

	return nil
}
