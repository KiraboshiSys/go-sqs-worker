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
	"github.com/mickamy/go-sqs-worker-example/internal/lib/logger"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := consumer.Config{
		UseDLQ:             true,
		WorkerQueueURL:     "https://sqs.ap-northeast-1.amazonaws.com/000000000000/worker-queue",
		DeadLetterQueueURL: "https://sqs.ap-northeast-1.amazonaws.com/000000000000/dead-letter-queue",
	}

	jobs := job.Jobs{
		SuccessfulJob: job.SuccessfulJob{},
	}
	getJobFunc := func(s string) (jobLib.Job, error) {
		return job.Get(s, jobs)
	}

	c, err := consumer.New(cfg, aws.NewSQSClient(ctx), getJobFunc, func(output consumer.ProcessingOutput) {
		if fatalErr := output.FatalError(); fatalErr != nil {
			logger.Error("fatal error occurred", "error", fatalErr, "message", output.Message)
		} else if nonFatalErr := output.NonFatalError(); nonFatalErr != nil {
			logger.Error("non-fatal error occurred", "error", nonFatalErr, "message", output.Message)
		} else {
			logger.Info("job processed successfully", "message", output.Message)
		}
	})
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
		return
	}

	workersCount := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			logger.Info(fmt.Sprintf("worker %d starting", workerID))
			c.Consume(ctx)
			logger.Info(fmt.Sprintf("worker %d finished", workerID))
		}(i)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("shutdown signal received, cancelling context")
	cancel()

	wg.Wait()
	logger.Info("all workers have finished, shutting down")
}
