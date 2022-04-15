package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	conduit "github.com/hamersaw/conduit-poc"
	protos "github.com/hamersaw/conduit-poc/protos/gen/pb-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	host = flag.String("host", "127.0.0.1", "The server host")
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	ctx := context.Background()

	// start network listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// intialize grpc server and register services
	server := grpc.NewServer()

	conduit := &Conduit{}
	protos.RegisterTaskServiceServer(server, conduit)
	protos.RegisterQueueServiceServer(server, conduit)

	// start grpc server
	log.Printf("server listening at %v", listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	ctx.Done()
}

type Conduit struct {
	queues sync.Map
}

func (c *Conduit) AddTask(ctx context.Context, request *protos.AddTaskRequest) (*protos.AddTaskResponse, error) {
	task := request.GetTask()

	// get queue for topic
	o, ok := c.queues.Load(task.GetTopic())
	if !ok {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("queue for topic '%s' does not exist", task.GetTopic()))
	}
	q, _ := o.(*conduit.Queue)

	// add task to queue
	if err := q.AddTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("failed to add task '%v' with err: %v", task.GetTopic(), err))
	}

	return &protos.AddTaskResponse{}, nil
}

func (c *Conduit) CreateQueue(ctx context.Context, request *protos.CreateQueueRequest) (*protos.CreateQueueResponse, error) {
	queue := request.GetQueue()

	// initialize and store new queue
	q := conduit.NewQueue(queue.GetTopic())
	if _, loaded := c.queues.LoadOrStore(queue.GetTopic(), &q); loaded {
		return nil, status.Errorf(codes.AlreadyExists, fmt.Sprintf("queue for topic '%s' already exists", queue.GetTopic()))
	}

	// start queue
	if err := q.Start(context.Background()); err != nil {
		c.queues.Delete(queue.GetTopic())
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("failed to start queue '%v' with err: %v", queue.GetTopic(), err))
	}

	return &protos.CreateQueueResponse{}, nil
}

func (c *Conduit) ListTopics(ctx context.Context, request *protos.ListTopicsRequest) (*protos.ListTopicsResponse, error) {
	// compile list of topics
	topics := make([]string, 0)
	c.queues.Range(func(key, value any) bool {
		if s, ok := key.(string); ok {
			topics = append(topics, s)
		}

		return true
	})

	return &protos.ListTopicsResponse{
		Topics: topics,
	}, nil
}
