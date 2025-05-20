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
	"time"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-playground/validator/v10"

	"github.com/mickamy/go-sqs-worker/internal/redis"
	internalScheduler "github.com/mickamy/go-sqs-worker/internal/scheduler"
	internalSQS "github.com/mickamy/go-sqs-worker/internal/sqs"
	"github.com/mickamy/go-sqs-worker/message"
)

// BeforeProduceFunc is a function that is called before a message is produced
type BeforeProduceFunc func(ctx context.Context, msg message.Message) error

// AfterProduceFunc is a function that is called after a message is produced
type AfterProduceFunc func(ctx context.Context, msg message.Message) error

var (
	validate = validator.New()
)

// Config is a configuration of the Producer
type Config struct {
	// WorkerQueueURL is the URL of the message queue
	//
	// This member is required
	WorkerQueueURL string

	// WorkerQueueARN is the ARN of the message queue
	//
	// This member is required if you use scheduling feature
	WorkerQueueARN string

	// SchedulerRoleARN is the ARN of the IAM role that EventBridge Scheduler will
	// use for when the schedule is invoked.
	//
	// This member is required if you use scheduling feature
	SchedulerRoleARN string

	// SchedulerTimeZone is the timezone of the scheduler
	//
	// This member is required if you use scheduling feature
	SchedulerTimeZone string

	// RedisURL is the URL of the Redis server
	// If not empty, the producer will store the message in Redis before enqueuing it to the worker queue
	//
	// This member is optional
	RedisURL string

	// BeforeProduceFunc is a function that is called before a message is produced
	//
	// This member is optional
	BeforeProduceFunc BeforeProduceFunc

	// AfterProduceFunc is a function that is called when a message is produced
	//
	// This member is optional
	AfterProduceFunc AfterProduceFunc
}

// Producer is a producer of the worker queue
type Producer struct {
	sqs       internalSQS.Client
	scheduler internalScheduler.Client
	redis     *redis.Client
	cfg       Config
}

// New creates a new Producer
func New(cfg Config, sqsClient *sqs.Client, scheduler *scheduler.Client) (*Producer, error) {
	if cfg.WorkerQueueURL == "" {
		return nil, fmt.Errorf("WorkerQueueURL is required")
	}
	if cfg.RedisURL != "" {
		rds, err := redis.New(redis.Config{
			URL: cfg.RedisURL,
			TTL: time.Hour * 24,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client: %w", err)
		}
		return &Producer{
			sqs:       internalSQS.New(sqsClient),
			scheduler: internalScheduler.New(scheduler, cfg.WorkerQueueARN, cfg.SchedulerRoleARN, cfg.SchedulerTimeZone),
			redis:     rds,
			cfg:       cfg,
		}, nil
	}
	return &Producer{
		sqs:       internalSQS.New(sqsClient),
		scheduler: internalScheduler.New(scheduler, cfg.WorkerQueueARN, cfg.SchedulerRoleARN, cfg.SchedulerTimeZone),
		cfg:       cfg,
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

	if err := p.beforeProduce(ctx, msg); err != nil {
		return err
	}

	if err := p.sqs.Enqueue(ctx, p.cfg.WorkerQueueURL, string(bytes)); err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	if err := p.afterProduce(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (p *Producer) DoScheduled(ctx context.Context, schedulerName string, msg message.Message, at time.Time) error {
	msg = setCaller(msg)

	if err := validate.StructCtx(ctx, msg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if schedulerName == "" {
		schedulerName = msg.ID.String()
	}

	if err := p.scheduler.EnqueueToSQS(ctx, schedulerName, msg, at); err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	if err := p.afterProduce(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (p *Producer) CancelSchedule(ctx context.Context, schedulerName string) error {
	if schedulerName == "" {
		return fmt.Errorf("schedule name is required")
	}

	if err := p.scheduler.Delete(ctx, schedulerName); err != nil {
		return fmt.Errorf("failed to cancel schedule: %w", err)
	}

	return nil
}

func (p *Producer) beforeProduce(ctx context.Context, msg message.Message) error {
	if p.redis != nil {
		if err := p.redis.SetMessage(ctx, msg); err != nil {
			return fmt.Errorf("failed to set message to Redis: %w", err)
		}
	}

	if p.cfg.BeforeProduceFunc != nil {
		if err := p.cfg.BeforeProduceFunc(ctx, msg); err != nil {
			return fmt.Errorf("failed to call before produce function: %w", err)
		}
	}

	return nil
}

func (p *Producer) afterProduce(ctx context.Context, msg message.Message) error {
	if p.cfg.AfterProduceFunc != nil {
		if err := p.cfg.AfterProduceFunc(ctx, msg); err != nil {
			return fmt.Errorf("failed to call after produce function: %w", err)
		}
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
