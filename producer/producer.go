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

	"github.com/mickamy/go-sqs-worker/internal/redis"
	internalSQS "github.com/mickamy/go-sqs-worker/internal/sqs"
	"github.com/mickamy/go-sqs-worker/message"
)

// OnProduceFunc is a function that is called when a message is produced
type OnProduceFunc func(msg message.Message)

var (
	validate = validator.New()
)

// Config is a configuration of the Producer
type Config struct {
	// WorkerQueueURL is the URL of the message queue
	WorkerQueueURL string

	// RedisURL is the URL of the Redis server
	// If not empty, the producer will store the message in Redis before enqueuing it to the worker queue
	RedisURL string

	// OnProduceFunc is a function that is called when a message is produced
	OnProduceFunc OnProduceFunc
}

// Producer is a producer of the worker queue
type Producer struct {
	client internalSQS.Client
	redis  *redis.Client
	cfg    Config
}

// New creates a new Producer
func New(cfg Config, client *sqs.Client) (*Producer, error) {
	if cfg.WorkerQueueURL == "" {
		return nil, fmt.Errorf("WorkerQueueURL is required")
	}
	if cfg.RedisURL != "" {
		rds, err := redis.New(cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client: %w", err)
		}
		return &Producer{
			client: internalSQS.New(client),
			redis:  rds,
			cfg:    cfg,
		}, nil
	}
	return &Producer{
		client: internalSQS.New(client),
		cfg:    cfg,
	}, nil
}

// Do produces a message to the worker queue
// It validates the message and enqueues it to the worker queue
// If the message is invalid, it returns an error
// If the enqueue fails, it returns an error
func (p *Producer) Do(ctx context.Context, msg message.Message) error {
	msg = setCaller(msg)

	if err := validate.StructCtx(ctx, msg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %s", err)
	}

	if p.redis != nil {
		if err := p.redis.SetMessage(ctx, msg); err != nil {
			return fmt.Errorf("failed to set message to Redis: %w", err)
		}
	}

	if err := p.client.Enqueue(ctx, p.cfg.WorkerQueueURL, string(bytes)); err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	if p.cfg.OnProduceFunc != nil {
		p.cfg.OnProduceFunc(msg)
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
