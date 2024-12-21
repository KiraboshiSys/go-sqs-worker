package job

import (
	"context"
	"errors"

	"github.com/mickamy/go-sqs-worker-example/internal/lib/logger"
)

type FailingJobPayload struct {
	Message string `json:"message"`
}

type FailingJob struct {
}

func (j FailingJob) Execute(ctx context.Context, payloadStr string) error {
	logger.Info("executing failing job", "payload", payloadStr)
	return errors.New("failing job failed")
}
