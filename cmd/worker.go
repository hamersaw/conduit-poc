package main

import (
	"context"
	"fmt"
	"flag"
	"log"
	"time"

	protos "github.com/hamersaw/conduit-poc/protos/gen/pb-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	host = flag.String("grpc-host", "127.0.0.1", "The grpc server host")
	port = flag.Int("grpc-port", 50051, "The grpc server port")
	topic = flag.String("topic", "foo", "The queue topic for this worker")
)

func main() {
	ctx := context.Background()

	// connect to grpc endpoint
	conn, err := grpcConnect(*host, *port)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC endpoint %s:%d", *host, *port)
	}
	defer conn.Close()

	// create TaskServiceClient
	client := protos.NewTaskServiceClient(conn)
	request := &protos.GetTaskRequest{
		Topic: *topic,
	}

	for {
		// send AddTaskRequest
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second * 10) // TODO - parameterize long poll duration
		defer cancel()

		response, err := client.GetTask(timeoutCtx, request)
		if err != nil {
			log.Printf("failed to retrieve task: %v", err)
			continue
		}

		task := response.GetTask()
		log.Printf("received task '%v'", *task)
		time.Sleep(task.GetExecutionDuration().AsDuration())
		log.Printf("completed task '%v'", *task)

		// TODO - send completion?
	}
}

func grpcConnect(host string, port int) (*grpc.ClientConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return conn, nil
}
