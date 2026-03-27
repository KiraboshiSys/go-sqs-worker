package redis_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/KiraboshiSys/go-sqs-worker/internal/redis"
	"github.com/KiraboshiSys/go-sqs-worker/message"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type jobType string

const (
	testJobType             jobType = "TestJob"
	successMessageExpiresIn         = 7 * 24 * time.Hour
	_messagesKey                    = "gsw:messages"
)

func caller() string {
	pcs := [13]uintptr{}
	length := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:length])
	frame, _ := frames.Next()
	return fmt.Sprintf("%s:%d", frame.Function, frame.Line)
}

func TestClient_UpdateMessage(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name           string
		arrangeMessage func(msg *message.Message)
		arrange        func(msg message.Message) message.Message
		assert         func(t *testing.T, ctx context.Context, redis *miniredis.Miniredis, msg message.Message, err error)
	}{
		{
			name: "successfully updates status to processing",
			arrange: func(msg message.Message) message.Message {
				return msg.Processing()
			},
			assert: func(t *testing.T, ctx context.Context, redis *miniredis.Miniredis, msg message.Message, err error) {
				assert.NoError(t, err)
				key := fmt.Sprintf("%s:%s:%s", _messagesKey, msg.ID.String(), message.Processing)
				require.True(t, redis.Exists(key))
			},
		},
		{
			name: "successfully updates status to success",
			arrange: func(msg message.Message) message.Message {
				return msg.Success()
			},
			assert: func(t *testing.T, ctx context.Context, redis *miniredis.Miniredis, msg message.Message, err error) {
				assert.NoError(t, err)
				key := fmt.Sprintf("%s:%s:%s", _messagesKey, msg.ID.String(), message.Success)
				require.True(t, redis.Exists(key))
				require.Equal(t, successMessageExpiresIn, redis.TTL(key))
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			mr := miniredis.RunT(t)
			redisURL := fmt.Sprintf("redis://%s", mr.Addr())
			sut, err := redis.New(redis.Config{URL: redisURL})

			msg := message.Message{
				ID:        uuid.New(),
				Type:      string(testJobType),
				Status:    message.Queued,
				Caller:    caller(),
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}

			require.NoError(t, sut.SetMessage(ctx, msg))
			require.NoError(t, err)

			next := tc.arrange(msg)

			tc.assert(t, ctx, mr, msg, sut.UpdateMessage(ctx, next))
		})
	}
}
