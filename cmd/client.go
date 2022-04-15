package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"os"

	protos "github.com/hamersaw/conduit-poc/protos/gen/pb-go"

	 cli "github.com/urfave/cli/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	app := &cli.App{
		Name:  "conduit",
		Usage: "PoC for a distributed durable task queue",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "grpc-host",
				Aliases: []string{"g"},
				Value:   "localhost",
			},
			&cli.IntFlag{
				Name:    "grpc-port",
				Aliases: []string{"p"},
				Value:   50051,
			},
		},
		Commands: []*cli.Command{
			{
				Name: "task",
				Usage: "create / modify / list task(s)",
				Subcommands: []*cli.Command{
					{
						Name:     "add",
						Aliases:  []string{"a"},
						Usage:    "add a task",
						Action:   addTask,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Aliases:  []string{"t"},
								Required: true,
							},
							&cli.IntFlag{
								Name:     "duration-ms",
								Aliases:  []string{"d"},
								Value:    1000,
								Required: true,
							},
						},
					},
				},
			},
			{
				Name: "queue",
				Usage: "create / modify / list queue(s)",
				Subcommands: []*cli.Command{
					{
						Name:     "create",
						Aliases:  []string{"c"},
						Usage:    "create a task queue",
						Action:   createQueue,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "topic",
								Aliases:  []string{"t"},
								Required: true,
							},
						},
					},
					{
						Name:     "list",
						Aliases:  []string{"l"},
						Usage:    "create a task queue",
						Action:   listQueues,
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func addTask(ctx *cli.Context) error {
	// connect to grpc endpoint
	conn, err := grpcConnect(ctx.String("grpc-host"), ctx.Int("grpc-port"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Failed to connect to gRPC endpoint %s:%d", ctx.String("grpc-host"), ctx.Int("grpc-port")), 1)
	}
	defer conn.Close()

	// create TaskServiceClient
	client := protos.NewTaskServiceClient(conn)

	// send AddTaskRequest
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second * 2)
	defer cancel()

	request := &protos.AddTaskRequest{
		Task: &protos.Task{
			Id:    "foo",
			Topic: ctx.String("topic"),
			// TODO hamersaw - add duration
		},
	}

	if _, err := client.AddTask(timeoutCtx, request); err != nil {
		return cli.Exit(fmt.Sprintf("Failed to add task %v with err: %v", request, err), 1)
	}

	return nil
}

func createQueue(ctx *cli.Context) error {
	// connect to grpc endpoint
	conn, err := grpcConnect(ctx.String("grpc-host"), ctx.Int("grpc-port"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Failed to connect to gRPC endpoint %s:%d", ctx.String("grpc-host"), ctx.Int("grpc-port")), 1)
	}
	defer conn.Close()

	// create QueueServiceClient
	client := protos.NewQueueServiceClient(conn)

	// send CreateQueueRequest
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second * 2)
	defer cancel()

	request := &protos.CreateQueueRequest{
		Queue: &protos.Queue{
			Topic: ctx.String("topic"),
		},
	}

	if _, err := client.CreateQueue(timeoutCtx, request); err != nil {
		return cli.Exit(fmt.Sprintf("Failed to create queue %v with err: %v", request, err), 1)
	}

	return nil
}

func listQueues(ctx *cli.Context) error {
	// connect to grpc endpoint
	conn, err := grpcConnect(ctx.String("grpc-host"), ctx.Int("grpc-port"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("Failed to connect to gRPC endpoint %s:%d", ctx.String("grpc-host"), ctx.Int("grpc-port")), 1)
	}
	defer conn.Close()

	// create QueueServiceClient
	client := protos.NewQueueServiceClient(conn)

	// send ListQueuesRequest
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second * 2)
	defer cancel()

	request := &protos.ListTopicsRequest{}
	response, err := client.ListTopics(timeoutCtx, request)
	if err != nil {
		return cli.Exit(fmt.Sprintf("Failed to list queues %v with err: %v", request, err), 1)
	}

	for _, topic := range response.GetTopics() {
		fmt.Println(topic)
	}

	return nil
}

func grpcConnect(host string, port int) (*grpc.ClientConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return conn, nil
}
