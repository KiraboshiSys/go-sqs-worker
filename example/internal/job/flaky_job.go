package job

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

type FlakyJobPayload struct {
	Message string `json:"message"`
}

type FlakyJob struct {
}

func (j FlakyJob) Execute(ctx context.Context, payloadStr string) error {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	if rand.Float64() < 0.3 {
		return errors.New("flaky job failed randomly")
	}

	return nil
}
