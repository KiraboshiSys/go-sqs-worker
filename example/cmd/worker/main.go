package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/mickamy/go-sqs-worker/consumer"
	jobLib "github.com/mickamy/go-sqs-worker/job"

	"github.com/mickamy/go-sqs-worker-example/internal/job"
	"github.com/mickamy/go-sqs-worker-example/internal/lib/aws"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := consumer.Config{
		WorkerQueueURL:     "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
		DeadLetterQueueURL: "http://localhost.localstack.cloud:4566/000000000000/dead-letter-queue",
	}

	jobs := job.Jobs{
		SuccessfulJob: job.SuccessfulJob{},
	}

	getJobFunc := func(s string) (jobLib.Job, error) {
		return job.Get(s, jobs)
	}

	c, err := consumer.New(cfg, aws.NewSQSClient(ctx), getJobFunc, func(output consumer.Output) {
		if fatalErr := output.FatalError(); fatalErr != nil {
			fmt.Println("fatal error occurred", "error", fatalErr, "message", output.Message)
		} else if nonFatalErr := output.NonFatalError(); nonFatalErr != nil {
			fmt.Println("non-fatal error occurred", "error", nonFatalErr, "message", output.Message)
		} else {
			fmt.Println("message processed successfully", "message", output.Message)
		}
	})
	if err != nil {
		fmt.Println("failed to create consumer", "error", err)
		os.Exit(1)
		return
	}

	workersCount := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fmt.Println("worker", workerID, "starting")
			c.Do(ctx)
			fmt.Println("worker", workerID, "finished")
		}(i)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("shutdown signal received, cancelling context")
	cancel()

	wg.Wait()
	fmt.Println("all workers have finished, shutting down")
}
