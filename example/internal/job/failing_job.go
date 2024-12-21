package job

import (
	"context"
	"errors"
)

type FailingJobPayload struct {
	Message string `json:"message"`
}

type FailingJob struct {
}

func (j FailingJob) Execute(ctx context.Context, payloadStr string) error {
	return errors.New("failing job failed")
}
