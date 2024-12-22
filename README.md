# go-sqs-worker

`go-sqs-worker` is a Go library designed to manage asynchronous jobs using AWS SQS. It provides a simple, scalable way to produce and consume tasks, making it ideal for background job processing.

## Features

- **Producer and Consumer Support**: Create and handle jobs with separate producer and consumer components.
- **Dead Letter Queue Integration**: Built-in support for DLQs to ensure reliable message processing.
- **Configurable Retry Mechanism**: Customize the number of retries and delay intervals for failed jobs.
- **Graceful Shutdown**: Handles context cancellation and cleanup during termination.
- **LocalStack Compatibility**: Works seamlessly with LocalStack for local development and testing. 

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
	"github.com/mickamy/go-sqs-worker/worker"

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

	msg, err := worker.NewMessage(ctx, job.SuccessfulJobType.String(), job.SuccessfulJobPayload{
		Message: "hello",
	})
	if err != nil {
		fmt.Println("failed to create successful job message", "error", err)
		return
	}
	if err := p.Produce(ctx, msg); err != nil {
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

	c, err := consumer.New(cfg, aws.NewSQSClient(ctx), job.GetJobHandler, func(output consumer.Output) {
		if fatalErr := output.FatalError(); fatalErr != nil {
			fmt.Println("fatal error occurred", "error", fatalErr, "message", output.Message)
		} else if nonFatalErr := output.NonFatalError(); nonFatalErr != nil {
			fmt.Println("non-fatal error occurred", "error", nonFatalErr, "message", output.Message)
		} else {
			fmt.Println("message processed successfully", "message", output.Message)
		}
	})

	if err != nil {
		fmt.Println("failed to create consumer", "error", err)
		return
	}

	c.Consume(ctx)
}


```


## Example Project

The repository includes an example project under the example/ directory. To run the example:

```bash
make up
```

## Configuration
The library uses the following configuration options:

- **WorkerQueueURL**: The URL of the SQS queue for worker messages.
- **DeadLetterQueueURL**: The URL of the DLQ for failed messages.
- **MaxRetry**: Maximum number of retries for a failed job (default: 3). 
- **BaseDelay**: Initial delay (in seconds) before retrying a failed job (default: 1.0). 
- **MaxDelay**: Maximum delay (in seconds) between retries (default: 60.0).
- **WaitTimeSeconds**: Maximum time to wait for a message to be received (default: 20). The maximum is [20 seconds](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).

## Documentation
GoDoc documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/mickamy/go-sqs-worker).

## License
This library is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
