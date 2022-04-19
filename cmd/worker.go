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

	// create TaskServiceClient
	client := proto.NewTaskServiceClient(conn)
	request := &proto.GetTaskRequest{
		Topic: *topic,
	}

	go func() {
		defer conn.Close()
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
			//log.Printf("received task '%v'", *task)
			time.Sleep(task.GetExecutionDuration().AsDuration())
			log.Printf("completed task '%v'", *task)

			// TODO - send completion?
		}
	}()

	return nil
}
