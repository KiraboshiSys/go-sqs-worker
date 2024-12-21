package job

import (
	"context"
)

type SuccessfulJobPayload struct {
	Message string `json:"message"`
}

type SuccessfulJob struct {
}

func (j SuccessfulJob) Execute(ctx context.Context, payloadStr string) error {
	return nil
}
