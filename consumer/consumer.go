/*
Package consumer provides structures and functions for consuming messages from an SQS queue and processing them.

The Output struct represents the result of processing a message, including the message itself, any error that occurred, and whether the error is fatal. The package includes functions for creating a new consumer, consuming messages, processing messages, retrying messages with exponential backoff, and sending messages to a dead letter queue.

Types:

  - Config: Configuration for the Consumer, including queue URLs, retry settings, and wait time.
  - BeforeProcessFunc: A function that is executed before processing a message.
  - AfterProcessFunc: A function that is executed after processing a message.
  - Consumer: Represents a consumer that retrieves and processes messages from the SQS queue.
  - Output: Represents the result of processing a message, including the message itself, any error that occurred, and whether the error is fatal.

Functions:

  - New: Creates a new Consumer with the given configuration, SQS client, job retrieval function, and process output handler.
  - Consumer.Do: Consumes messages from the worker queue and processes them.
  - Consumer.Process: Processes a single message and returns the processing output.
  - Output.FatalError: Returns the error if the output is fatal, otherwise nil.
  - Output.NonFatalError: Returns the error if the output is not fatal, otherwise nil.

Usage:

To create a new consumer, use the New function:

	c, err := consumer.New(cfg, sqsClient, job.GetJobHandler)
	if err != nil {
	    // handle error
	}

To start consuming messages, use the Consumer.Do method:

	c.Do(ctx)

To process a single message, use the Consumer.Process method:

	output := c.Process(ctx, messageString)
*/
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-playground/validator/v10"

	"github.com/mickamy/go-sqs-worker/internal/ptr"
	"github.com/mickamy/go-sqs-worker/internal/redis"
	internalSQS "github.com/mickamy/go-sqs-worker/internal/sqs"
	"github.com/mickamy/go-sqs-worker/job"
	"github.com/mickamy/go-sqs-worker/message"
)

var (
	validate = validator.New()

	ErrSuccessfullyRetried = errors.New("failed to execute job; retried successfully")
)

// BeforeProcessFunc is a function that is executed before processing a message.
type BeforeProcessFunc func(ctx context.Context, msg message.Message) error

// AfterProcessFunc is a function that is executed after processing a message.
type AfterProcessFunc func(ctx context.Context, output Output) error

// Config represents the configuration for a Consumer.
// It includes settings for the worker queue, dead letter queue, retry logic, and SQS-specific options.
type Config struct {
	// WorkerQueueURL is the URL of the worker queue.
	// This queue is used to store messages that need to be processed by the worker.
	// It is a required configuration parameter.
	WorkerQueueURL string

	// DeadLetterQueueURL is the URL of the dead letter queue.
	// If not set, the dead letter queue is not used.
	// Messages that fail to process after the maximum number of retries are sent to this queue.
	DeadLetterQueueURL string

	// RedisURL is the URL of the Redis server.
	// If set, data will be stored both before and after message processing.
	// This is particularly useful for tracking job-related data during processing, especially when used with [go-sqs-worker-viewer].
	//
	// [go-sqs-worker-viewer]: https://github.com/mickamy/go-sqs-worker-viewer
	RedisURL string

	// MaxRetry is the maximum number of retries for a failed job.
	// If not set, the default value is 5 retries.
	MaxRetry int

	// BaseDelay is the initial delay (in seconds) before retrying a failed job.
	// This value is used as the base for calculating exponential backoff delays.
	// If not set, the default value is 30 seconds.
	BaseDelay float64

	// MaxDelay is the maximum delay (in seconds) between retries.
	// This value is used to cap the exponential backoff delay.
	// If not set, the default value is 3600 seconds (1 hour).
	MaxDelay int

	// WaitTimeSeconds is the maximum time (in seconds) to wait for a message to be received from the SQS queue.
	// This value is used for long polling. The maximum allowed value is 20 seconds.
	// If not set, the default value is 20 seconds.
	// For more information, see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
	WaitTimeSeconds int

	// BeforeProcessFunc is a function that is executed before processing a message.
	// This function can be used to perform custom logic before processing the message.
	// If an error is returned, the message will not be processed.
	BeforeProcessFunc BeforeProcessFunc

	// AfterProcessFunc is a function that is executed after processing a message.
	// This function can be used to perform custom logic after processing the message.
	// If an error is returned, the message will be enqueue to the worker queue again.
	AfterProcessFunc AfterProcessFunc
}

func (c Config) useDLQ() bool {
	return c.DeadLetterQueueURL != ""
}

func (c Config) useRedis() bool {
	return c.RedisURL != ""
}

func newConfig(c Config) Config {
	if c.MaxRetry == 0 {
		c.MaxRetry = 5
	}
	if c.BaseDelay == 0 {
		c.BaseDelay = 30
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 3600
	}
	if c.WaitTimeSeconds == 0 {
		c.WaitTimeSeconds = 20
	}
	if c.BeforeProcessFunc == nil {
		c.BeforeProcessFunc = func(context.Context, message.Message) error { return nil }
	}
	if c.AfterProcessFunc == nil {
		c.AfterProcessFunc = func(context.Context, Output) error { return nil }
	}

	return c
}

// Consumer represents a consumer that retrieves and processes messages from the SQS queue.
type Consumer struct {
	cfg        Config
	sqsClient  internalSQS.Client
	getJobFunc job.GetFunc
	redis      *redis.Client
}

// New creates a new Consumer
func New(cfg Config, client *sqs.Client, getJobFunc job.GetFunc) (*Consumer, error) {
	return newConsumer(cfg, internalSQS.New(client), getJobFunc)
}

func newConsumer(cfg Config, client internalSQS.Client, getJobFunc job.GetFunc) (*Consumer, error) {
	if getJobFunc == nil {
		return nil, fmt.Errorf("getJobFunc is required")
	}
	cfg = newConfig(cfg)
	if cfg.useRedis() {
		rds, err := redis.New(redis.Config{
			URL: cfg.RedisURL,
			TTL: time.Hour * 24,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client: %w", err)
		}
		return &Consumer{
			cfg:        cfg,
			sqsClient:  client,
			getJobFunc: getJobFunc,
			redis:      rds,
		}, nil
	}
	return &Consumer{
		cfg:        cfg,
		sqsClient:  client,
		getJobFunc: getJobFunc,
	}, nil
}

// Do continuously retrieves and processes messages from the worker queue until the context is canceled.
// It uses long polling to wait for messages and processes each message using the configured job handler.
// If a message fails to process, it will be retried based on the configured retry logic.
// If the maximum number of retries is reached, the message will be sent to the dead letter queue (DLQ) if configured.
// The OnProcessFunc callback is called after each message is processed, allowing custom handling of the output.
func (c *Consumer) Do(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, deleteMessage, err := c.sqsClient.Dequeue(ctx, c.cfg.WorkerQueueURL, c.cfg.WaitTimeSeconds)
			if err != nil {
				// continue processing if dequeue failed
				continue
			}
			if m == nil {
				// continue processing if message is empty
				continue
			}

			// delete message before processing to avoid duplicate processing
			if err := deleteMessage(ctx); err != nil {
				continue
			}

			output := c.Process(ctx, *m)
			if afterProcessErr := c.afterProcess(ctx, output); afterProcessErr != nil {
				c.retry(ctx, output.Message)
			}
		}
	}
}

// Process processes a single message string and returns the processing output.
// It unmarshals the message, retrieves the corresponding job, and executes it.
// If an error occurs during unmarshalling or job retrieval, the message is sent to the DLQ if configured.
// If the job execution fails, it retries the job based on the configured retry logic.
// If the maximum number of retries is reached, the message is sent to the DLQ if configured.
// The function recovers from panics and returns a fatal error in such cases.
func (c *Consumer) Process(ctx context.Context, s string) (output Output) {
	defer func() {
		if r := recover(); r != nil {
			output = fatalOutput(fmt.Errorf("panic occurred while processing message: %v", r))
		}
	}()

	msg, err := parse(ctx, s)
	if err != nil {
		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalOutput(
				fmt.Errorf("failed to unmarshal message and send to DLQ: %w", dlqErr),
			)
		}
		return nonFatalOutput(
			fmt.Errorf("failed to unmarshal message; sent to DLQ successfully: %w", err),
		)
	}

	return c.ProcessMessage(ctx, msg)
}

// ProcessMessage processes a single message and returns the processing output.
// It retrieves the corresponding job, executes it, and handles retries and DLQ processing.
// If an error occurs during job retrieval, the message is sent to the DLQ if configured.
// If the job execution fails, it retries the job based on the configured retry logic.
// If the maximum number of retries is reached, the message is sent to the DLQ if configured.
// The function recovers from panics and returns a fatal error in such cases.
func (c *Consumer) ProcessMessage(ctx context.Context, msg message.Message) (output Output) {
	defer func() {
		if r := recover(); r != nil {
			output = fatalOutput(fmt.Errorf("panic occurred while processing message: %v", r))
		}
	}()

	if beforeProcessErr := c.beforeProcess(ctx, msg); beforeProcessErr != nil {
		return nonFatalOutput(beforeProcessErr).withMessage(msg)
	}

	j, err := c.getJobFunc(msg.Type)
	if err != nil {
		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalOutput(
				fmt.Errorf("failed to get job and send to DLQ: %w", dlqErr),
			).withMessage(msg)
		}
		return nonFatalOutput(
			fmt.Errorf("failed to get job; sent to DLQ successfully: %w", err),
		).withMessage(msg)
	}

	return c.execute(ctx, j, msg)
}

// execute executes a job and returns the Output
func (c *Consumer) execute(ctx context.Context, j job.Job, msg message.Message) Output {
	if err := j.Execute(ctx, msg.Payload); err != nil {
		if msg.RetryCount < c.cfg.MaxRetry {
			return c.retry(ctx, msg)
		}

		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalOutput(
				fmt.Errorf("max retry attempts reached; failed to send to DLQ: %w", dlqErr),
			).withMessage(msg.Failed())
		}
		return nonFatalOutput(
			fmt.Errorf("max retry attempts exceeded; sent to DLQ successfully: %w", err),
		).withMessage(msg.Failed())
	}
	return Output{
		Message: msg.Success(),
	}
}

// retry retries a job. If the retry fails, it sends the message to the dead letter queue.
func (c *Consumer) retry(ctx context.Context, msg message.Message) Output {
	if retryErr := c.doRetry(ctx, &msg); retryErr != nil {
		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalOutput(
				fmt.Errorf("failed to execute job and retry and send to DLQ: %w", dlqErr),
			).withMessage(msg.Failed())
		}
		return nonFatalOutput(
			fmt.Errorf("failed to execute job and retry; sent to DLQ successfully: %w", retryErr),
		).withMessage(msg.Failed())
	}
	return nonFatalOutput(ErrSuccessfullyRetried).withMessage(msg.Retrying())
}

// doRetry retries a job with exponential backoff
func (c *Consumer) doRetry(ctx context.Context, msg *message.Message) error {
	msg = ptr.Of(msg.Retrying())
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message on retry: %w", err)
	}
	if enqueueErr := c.sqsClient.EnqueueWithDelay(ctx, c.cfg.WorkerQueueURL, string(bytes), c.calculateBackoff(msg.RetryCount)); enqueueErr != nil {
		return fmt.Errorf("faild to enqueue on retry: %w", enqueueErr)
	}
	return nil
}

// sendToDLQ sends a message to the dead letter queue
func (c *Consumer) sendToDLQ(ctx context.Context, msg message.Message) error {
	if !c.cfg.useDLQ() {
		return nil
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshalling message on sending to DLQ: %w", err)
	}
	if err := c.sqsClient.Enqueue(ctx, c.cfg.DeadLetterQueueURL, string(bytes)); err != nil {
		return fmt.Errorf("failed to enqueue message on sending to DLQ: %w", err)
	}
	return nil
}

// calculateBackOff calculates exponential backoff
func (c *Consumer) calculateBackoff(retries int) int {
	delay := c.cfg.BaseDelay * math.Pow(2, float64(retries-1))
	return int(math.Min(delay, float64(c.cfg.MaxDelay)))
}

func (c *Consumer) beforeProcess(ctx context.Context, msg message.Message) error {
	if c.cfg.useRedis() {
		if err := c.redis.SetMessage(ctx, msg.Processing()); err != nil {
			return fmt.Errorf("failed to update status to processing: %w", err)
		}
	}
	if err := c.cfg.BeforeProcessFunc(ctx, msg); err != nil {
		return fmt.Errorf("before process failed: %w", err)
	}
	return nil
}

func (c *Consumer) afterProcess(ctx context.Context, output Output) error {
	if c.cfg.useRedis() {
		if err := c.redis.SetMessage(ctx, output.Message); err != nil {
			return fmt.Errorf("failed to update status after processing: %w", err)
		}
	}
	if err := c.cfg.AfterProcessFunc(ctx, output); err != nil {
		return fmt.Errorf("after process failed: %w", err)
	}
	return nil
}

// parse parses a message to a message.Message
func parse(ctx context.Context, s string) (message.Message, error) {
	var msg message.Message
	if err := json.Unmarshal([]byte(s), &msg); err != nil {
		return message.Message{}, fmt.Errorf("failed to unmarshalling message: %w", err)
	}
	if err := validate.StructCtx(ctx, msg); err != nil {
		return message.Message{}, fmt.Errorf("failed to validate message: %s", err)
	}
	return msg, nil
}
