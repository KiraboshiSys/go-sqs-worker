package job

import (
	"context"

	"github.com/mickamy/go-sqs-worker-example/internal/lib/logger"
)

type SuccessfulJob struct {
}

func (j SuccessfulJob) Execute(ctx context.Context, payloadStr string) error {
	logger.Info("executing successful job", "payload", payloadStr)
	return nil
}
