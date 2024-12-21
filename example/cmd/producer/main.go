package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mickamy/go-sqs-worker/producer"
	"github.com/mickamy/go-sqs-worker/worker"

	"github.com/mickamy/go-sqs-worker-example/internal/job"
	"github.com/mickamy/go-sqs-worker-example/internal/lib/aws"
	"github.com/mickamy/go-sqs-worker-example/internal/lib/logger"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		logger.Info("received signal, shutting down")
		cancel()
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	p := producer.New(producer.Config{
		WorkerQueueURL: "https://sqs.ap-northeast-1.amazonaws.com/000000000000/worker-queue",
	}, aws.NewSQSClient(ctx))
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down producer")
			return
		case t := <-ticker.C:
			switch t.Second() % 3 {
			case 0:
				msg, err := worker.NewMessage(ctx, job.FailingJobType.String(), job.FailingJobPayload{
					Message: "hello failing job",
				})
				if err != nil {
					logger.Error("failed to create failing job message", "error", err)
					continue
				}
				if err := p.Produce(ctx, msg); err != nil {
					logger.Error("failed to produce failing job", "error", err)
					continue
				}
			case 1:
				msg, err := worker.NewMessage(ctx, job.FlakyJobType.String(), job.FlakyJobPayload{
					Message: "hello flaky job",
				})
				if err != nil {
					logger.Error("failed to create flaky job message", "error", err)
					continue
				}
				if err := p.Produce(ctx, msg); err != nil {
					logger.Error("failed to produce flaky job", "error", err)
					continue
				}
			case 2:
				msg, err := worker.NewMessage(ctx, job.FailingJobType.String(), job.SuccessfulJobPayload{
					Message: "hello",
				})
				if err != nil {
					logger.Error("failed to create successful job message", "error", err)
					continue
				}
				if err := p.Produce(ctx, msg); err != nil {
					logger.Error("failed to produce successful job", "error", err)
					continue
				}
			}
		}
	}
}
