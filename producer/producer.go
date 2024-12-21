package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-playground/validator/v10"

	"github.com/mickamy/go-sqs-worker/worker"
)

var (
	validate = validator.New()
)

// Config is a configuration of the Producer
type Config struct {
	// WorkerQueueURL is the URL of the worker queue
	WorkerQueueURL string
}

// Producer is a producer of the worker queue
type Producer struct {
	client         *sqs.Client
	workerQueueURL string
}

// NewProducer creates a new Producer
func NewProducer(cfg Config, client *sqs.Client) *Producer {
	return &Producer{
		client:         client,
		workerQueueURL: cfg.WorkerQueueURL,
	}
}

func (p *Producer) Produce(ctx context.Context, msg worker.Message) error {
	frame := callerFrame()
	caller := fmt.Sprintf("%s:%d", frame.File, frame.Line)
	msg.SetCaller(caller)

	if err := validate.StructCtx(ctx, msg); err != nil {
		return fmt.Errorf("validation failed: %v", err)
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshalling failed: %s", err)
	}

	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(string(bytes)),
		QueueUrl:    aws.String(p.workerQueueURL),
	})
	if err != nil {
		return fmt.Errorf("enqueue failed: %w", err)
	}

	return nil
}

// callerFrame returns the frame of the caller
func callerFrame() runtime.Frame {
	pcs := [13]uintptr{}
	length := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:length])
	frame, _ := frames.Next()
	return frame
}
