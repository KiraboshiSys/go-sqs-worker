package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mickamy/go-sqs-worker/message"
	"github.com/mickamy/go-sqs-worker/producer"

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		fmt.Println("received signal, shutting down")
		cancel()
	}()

	failingJobTicker := time.NewTicker(3 * time.Second)
	flakyJobTicker := time.NewTicker(5 * time.Second)
	heavyJobTicker := time.NewTicker(7 * time.Second)
	successfulJobTicker := time.NewTicker(11 * time.Second)

	defer failingJobTicker.Stop()
	defer flakyJobTicker.Stop()
	defer successfulJobTicker.Stop()

	cfg := producer.Config{
		WorkerQueueURL: "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
		RedisURL:       redisURL,
		AfterProduceFunc: func(msg message.Message) {
			fmt.Println("produced message", "id", msg.ID)
		},
	}

	p, err := producer.New(cfg, aws.NewSQSClient(ctx))
	if err != nil {
		fmt.Println("failed to create producer", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutting down producer")
			return
		case <-failingJobTicker.C:
			msg, err := message.New(ctx, job.FailingJobType.String(), job.FailingJobPayload{
				Message: "hello failing job",
			})
			if err != nil {
				fmt.Println("failed to create failing job message", "error", err)
				continue
			}
			if err := p.Do(ctx, msg); err != nil {
				fmt.Println("failed to produce failing job", "error", err)
				continue
			}
		case <-flakyJobTicker.C:
			msg, err := message.New(ctx, job.FlakyJobType.String(), job.FlakyJobPayload{
				Message: "hello flaky job",
			})
			if err != nil {
				fmt.Println("failed to create flaky job message", "error", err)
				continue
			}
			if err := p.Do(ctx, msg); err != nil {
				fmt.Println("failed to produce flaky job", "error", err)
				continue
			}
		case <-heavyJobTicker.C:
			msg, err := message.New(ctx, job.HeavyJobType.String(), job.HeavyJobPayload{
				Message: "hello heavy job",
			})
			if err != nil {
				fmt.Println("failed to create heavy job message", "error", err)
				continue
			}
			if err := p.Do(ctx, msg); err != nil {
				fmt.Println("failed to produce heavy job", "error", err)
				continue
			}
		case <-successfulJobTicker.C:
			msg, err := message.New(ctx, job.SuccessfulJobType.String(), job.SuccessfulJobPayload{
				Message: "hello",
			})
			if err != nil {
				fmt.Println("failed to create successful job message", "error", err)
				continue
			}
			if err := p.Do(ctx, msg); err != nil {
				fmt.Println("failed to produce successful job", "error", err)
				continue
			}
		}
	}
}
