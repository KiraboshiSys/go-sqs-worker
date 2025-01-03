package job

import (
	"context"
	"time"

	"github.com/mickamy/go-sqs-worker/job"
)

type HeavyJobPayload struct {
	Message string `json:"message"`
}

type HeavyJob struct {
}

func (j HeavyJob) Execute(ctx context.Context, payloadStr string) error {
	time.Sleep(10 * time.Second)
	return nil
}

var _ job.Job = (*HeavyJob)(nil)
