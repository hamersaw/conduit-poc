package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hamersaw/conduit-poc/queue"
)

func main() {
	ctx := context.Background()

	q := queue.NewQueue("foo")
	q.Start(ctx)

	// wait on SIGTERM to close server end context
	c := make(chan os.Signal)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	ctx.Done()
}
