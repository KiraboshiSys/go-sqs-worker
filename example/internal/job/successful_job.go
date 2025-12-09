package job

import (
	"context"

	"github.com/KiraboshiSys/go-sqs-worker/job"
)

type SuccessfulJobPayload struct {
	Message string `json:"message"`
}

type SuccessfulJob struct {
}

func (j SuccessfulJob) Execute(ctx context.Context, payloadStr string) error {
	return nil
}

var _ job.Job = (*SuccessfulJob)(nil)
