/*
Package producer provides structures and functions for producing messages to an SQS queue.

The Producer struct represents a producer that sends messages to the SQS queue.
The package includes functions for creating a new producer, producing messages, and setting caller information.

Types:

  - Config: Configuration for the Producer, including the worker queue URL.
  - Producer: Represents a producer that sends messages to the SQS queue.

Functions:

  - New: Creates a new Producer with the given configuration and SQS client.
  - Producer.Do: Produces a message to the worker queue.
  - setCaller: Sets the caller information of a message.

Usage:

To create a new producer, use the New function:

	p, err := producer.New(config, sqsClient)
	if err != nil {
	    // handle error
	}

To produce a message, use the Producer.Do method:

	err := p.Do(ctx, msg)
	if err != nil {
	    // handle error
	}
*/
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-playground/validator/v10"

	internalSQS "github.com/mickamy/go-sqs-worker/internal/sqs"
	"github.com/mickamy/go-sqs-worker/message"
)

var (
	validate = validator.New()
)

// Config is a configuration of the Producer
type Config struct {
	// WorkerQueueURL is the URL of the message queue
	WorkerQueueURL string
}

// Producer is a producer of the worker queue
type Producer struct {
	client         internalSQS.Client
	workerQueueURL string
}

// New creates a new Producer
func New(cfg Config, client *sqs.Client) (*Producer, error) {
	if cfg.WorkerQueueURL == "" {
		return nil, fmt.Errorf("WorkerQueueURL is required")
	}
	return &Producer{
		client:         internalSQS.New(client),
		workerQueueURL: cfg.WorkerQueueURL,
	}, nil
}

// Do produces a message to the worker queue
// It validates the message and enqueues it to the worker queue
// If the message is invalid, it returns an error
// If the enqueue fails, it returns an error
func (p *Producer) Do(ctx context.Context, msg message.Message) error {
	msg = setCaller(msg)

	if err := validate.StructCtx(ctx, msg); err != nil {
		return fmt.Errorf("validation failed: %v", err)
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %s", err)
	}

	if err := p.client.Enqueue(ctx, p.workerQueueURL, string(bytes)); err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

// setCaller sets the caller information of a message
func setCaller(msg message.Message) message.Message {
	frame := callerFrame()
	caller := fmt.Sprintf("%s:%d", frame.File, frame.Line)
	msg.SetCaller(caller)
	return msg
}

// callerFrame returns the frame of the caller
func callerFrame() runtime.Frame {
	pcs := [13]uintptr{}
	length := runtime.Callers(4, pcs[:])
	frames := runtime.CallersFrames(pcs[:length])
	frame, _ := frames.Next()
	return frame
}
