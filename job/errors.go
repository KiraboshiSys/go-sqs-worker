package job

import (
	"errors"
)

var (
	ErrNonRetryable = errors.New("non-retryable error occurred during job execution")
)
