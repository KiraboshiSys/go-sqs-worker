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
	"github.com/mickamy/go-sqs-worker/message"

	"github.com/mickamy/go-sqs-worker-example/internal/job"
	"github.com/mickamy/go-sqs-worker-example/internal/lib/aws"
)

func main() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		fmt.Println("REDIS_URL is required")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := consumer.Config{
		WorkerQueueURL:     "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
		DeadLetterQueueURL: "http://localhost.localstack.cloud:4566/000000000000/dead-letter-queue",
		RedisURL:           redisURL,
		MaxRetry:           3,
		BaseDelay:          1,
		MaxDelay:           10,
		BeforeProcessFunc: func(ctx context.Context, msg message.Message) (context.Context, error) {
			fmt.Println("processing message", "id", msg.ID)
			return ctx, nil
		},
		AfterProcessFunc: func(ctx context.Context, output consumer.Output) (context.Context, error) {
			if err := output.FatalError(); err != nil {
				fmt.Println("fatal error occurred", "id", output.Message.ID, "error", err)
				return ctx, nil
			} else if err := output.NonFatalError(); err != nil {
				fmt.Println("non-fatal error occurred", "id", output.Message.ID, "error", err)
				return ctx, nil
			} else {
				fmt.Println("message processed", "id", output.Message.ID)
				return ctx, nil
			}
		},
	}

	jobs := job.Jobs{
		FailingJob:    job.FailingJob{},
		FlakyJob:      job.FlakyJob{},
		HeavyJob:      job.HeavyJob{},
		SuccessfulJob: job.SuccessfulJob{},
	}

	getJobFunc := func(s string) (jobLib.Job, error) {
		return job.Get(s, jobs)
	}

	c, err := consumer.New(cfg, aws.NewSQSClient(ctx), getJobFunc)
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
	fmt.Println("shutdown signal received, canceling context")
	cancel()

	wg.Wait()
	fmt.Println("all workers have finished, shutting down")
}
