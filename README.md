# go-sqs-worker

`go-sqs-worker` is a Go library designed to manage asynchronous jobs using AWS SQS. It provides a simple, scalable way to produce and consume tasks, making it ideal for background job processing.

## Features

- **Producer and Consumer Support**: Enables the creation and handling of jobs with distinct producer and consumer components.
- **Dead Letter Queue Integration**: Provides built-in support for Dead Letter Queues (DLQs) to ensure reliable message processing.
- **Configurable Retry Mechanism**: Allows customization of the number of retries and delay intervals for failed jobs.
- **Graceful Shutdown**: Manages context cancellation and cleanup during termination.
- **LocalStack Compatibility**: Fully compatible with LocalStack for local development and testing. 

## Installation

Add the library to your project using `go get`:

```bash
go get github.com/mickamy/go-sqs-worker
```

## Usage

### Producer Example

The producer is responsible for sending jobs to the SQS queue.

```go
package main

import (
	"context"
	"fmt"

	"github.com/mickamy/go-sqs-worker/producer"
	"github.com/mickamy/go-sqs-worker/message"

	"github.com/mickamy/go-sqs-worker-example/internal/job"
	"github.com/mickamy/go-sqs-worker-example/internal/lib/aws"
)

func main() {
	ctx := context.Background()

	cfg := producer.Config{
		WorkerQueueURL: "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
	}

	p, err := producer.New(cfg, aws.NewSQSClient(ctx))
	if err != nil {
		fmt.Println("failed to create producer", "error", err)
		return
	}

	msg, err := message.New(ctx, job.SuccessfulJobType.String(), job.SuccessfulJobPayload{
		Message: "hello",
	})
	if err != nil {
		fmt.Println("failed to create successful job message", "error", err)
		return
	}
	if err := p.Do(ctx, msg); err != nil {
		fmt.Println("failed to produce successful job", "error", err)
		return
	}
}

```

### Consumer Example

The consumer retrieves and processes jobs from the SQS queue.

```go
package main

import (
	"context"
	"fmt"

	"github.com/mickamy/go-sqs-worker-example/internal/lib/aws"
	"github.com/mickamy/go-sqs-worker/consumer"
)

func main() {
	ctx := context.Background()

	cfg := consumer.Config{
		WorkerQueueURL:     "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
		DeadLetterQueueURL: "http://localhost.localstack.cloud:4566/000000000000/dead-letter-queue",
	}

	c, err := consumer.New(cfg, aws.NewSQSClient(ctx), job.GetJobHandler)

	if err != nil {
		fmt.Println("failed to create consumer", "error", err)
		return
	}

	c.Do(ctx)
}


```


## Example Project

The repository includes an example project under the example/ directory. To run the example:

```bash
make up
```

## Configuration
The library uses the following configuration options:

- **WorkerQueueURL**: Required. The URL of the SQS queue where worker messages are stored.
- **DeadLetterQueueURL**: Optional. The URL of the Dead Letter Queue (DLQ) for messages that fail to process after the maximum number of retries. If not set, the DLQ is not used.
- **MaxRetry**: Optional. The maximum number of retry attempts for a failed job. The default value is 5 retries.
- **BaseDelay**: Optional. The initial delay (in seconds) before retrying a failed job. This value is used as the base for calculating exponential backoff delays. The default value is 30 seconds.
- **MaxDelay**: Optional. The maximum delay (in seconds) between retries, used to cap the exponential backoff delay. The default value is 3600 seconds (1 hour).
- **WaitTimeSeconds**: Optional. The maximum time (in seconds) to wait for a message to be received from the SQS queue. This value is used for long polling. The default value is 20 seconds. The maximum allowed value is [20 seconds](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).
- **OnProcessFunc**: Optional. A function to execute after a message is processed. The default function does nothing.

## Documentation
GoDoc documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/mickamy/go-sqs-worker).

## License
This library is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
