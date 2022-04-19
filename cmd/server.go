package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	conduit "github.com/hamersaw/conduit-poc"
	proto "github.com/hamersaw/conduit-poc/protos/gen/pb-go"

	"github.com/uptrace/bun"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	dbHost = flag.String("db-host", "127.0.0.1", "The DB server host")
	dbName = flag.String("db-name", "conduit", "The DB name")
	dbPassword = flag.String("db-password", "foo", "The DB password")
	dbPort = flag.Int("db-port", 5432, "The DB server port")
	dbUsername = flag.String("db-username", "postgres", "The DB username")
	host = flag.String("host", "127.0.0.1", "The server host")
	port = flag.Int("port", 50051, "The server port")
	ingressBufferSize = flag.Int("ingress-buffer-size", 100, "The maximum number of items allowed in the ingress buffer")
	queueBufferSize = flag.Int("queue-buffer-size", 20, "The maximum number of items allowed in each queue buffer")
)

func main() {
	ctx := context.Background()

	// start network listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// connect to database
	db, err := conduit.OpenDB(ctx, *dbHost, *dbPort, *dbUsername, *dbPassword, *dbName)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	// initalize and start Ingress
	var queues sync.Map
	ingress := conduit.NewIngress(db, &queues, *ingressBufferSize)
	ingress.Start(ctx)

	// intialize grpc server and register services
	conduit := &Conduit{
		db:      db,
		ingress: &ingress,
		queues:  &queues,
	}

	server := grpc.NewServer()
	proto.RegisterTaskServiceServer(server, conduit)
	proto.RegisterQueueServiceServer(server, conduit)

	// start grpc server
	log.Printf("server listening at %v", listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	ctx.Done()
}

type Conduit struct {
	db      *bun.DB
	ingress *conduit.Ingress
	queues  *sync.Map
}

func (c *Conduit) AddTask(ctx context.Context, request *proto.AddTaskRequest) (*proto.AddTaskResponse, error) {
	now := time.Now()
	task := conduit.FromProto(*request.GetTask())
	task.InitializedAt = &now

	if err := c.ingress.AddTask(ctx, task); err != nil {
		// if error is compatible with grpc return directly
		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		return nil, status.Errorf(codes.Internal, fmt.Sprintf("failed to add task '%v' with err: %v", *request.GetTask(), err))
	}

	return &proto.AddTaskResponse{}, nil
}

func (c *Conduit) CreateQueue(ctx context.Context, request *proto.CreateQueueRequest) (*proto.CreateQueueResponse, error) {
	queue := request.GetQueue()

	// initialize and store new queue
	q := conduit.NewQueue(*queueBufferSize, queue.GetTopic())
	if _, loaded := c.queues.LoadOrStore(queue.GetTopic(), &q); loaded {
		return nil, status.Errorf(codes.AlreadyExists, fmt.Sprintf("queue for topic '%s' already exists", queue.GetTopic()))
	}

	// start queue
	if err := q.Start(context.Background(), c.db); err != nil {
		c.queues.Delete(queue.GetTopic())
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("failed to start queue '%v' with err: %v", queue.GetTopic(), err))
	}

	return &proto.CreateQueueResponse{}, nil
}

func (c *Conduit) GetTask(ctx context.Context, request *proto.GetTaskRequest) (*proto.GetTaskResponse, error) {
	topic := request.GetTopic()

	// get queue for topic
	o, ok := c.queues.Load(topic)
	if !ok {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("queue for topic '%s' does not exist", topic))
	}
	q, _ := o.(*conduit.Queue)

	// get task
	task, err := q.GetTask(ctx)
	if err != nil {
		// if error is compatible with grpc return directly
		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		return nil, status.Errorf(codes.Internal, fmt.Sprintf("failed to get task for topic '%v' with err: %v", topic, err))
	}

	return &proto.GetTaskResponse{
		Task: task.ToProto(),
	}, nil
}

func (c *Conduit) ListTopics(ctx context.Context, request *proto.ListTopicsRequest) (*proto.ListTopicsResponse, error) {
	// compile list of topics
	topics := make([]string, 0)
	c.queues.Range(func(key, value any) bool {
		if s, ok := key.(string); ok {
			topics = append(topics, s)
		}

		return true
	})

	return &proto.ListTopicsResponse{
		Topics: topics,
	}, nil
}
