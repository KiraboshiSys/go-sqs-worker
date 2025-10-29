package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/mickamy/go-sqs-worker/internal/redis"
	"github.com/mickamy/go-sqs-worker/internal/sqs/mock_sqs"
	"github.com/mickamy/go-sqs-worker/job"
	"github.com/mickamy/go-sqs-worker/message"
)

type jobType string

const (
	testJobType jobType = "TestJob"
)

func getJob(s string) (job.Job, error) {
	if s == string(testJobType) {
		return testJob{}, nil
	}
	return nil, errors.New("job not found")
}

type testJob struct {
	ExecuteFunc func() error
}

type schedulerSpy struct {
	called       bool
	scheduleName string
	message      message.Message
	runAt        time.Time
	err          error
}

func (s *schedulerSpy) EnqueueToSQS(ctx context.Context, scheduleName string, msg message.Message, at time.Time) error {
	s.called = true
	s.scheduleName = scheduleName
	s.message = msg
	s.runAt = at
	return s.err
}

func (s *schedulerSpy) Delete(ctx context.Context, scheduleName string) error {
	return nil
}

func (j testJob) Execute(ctx context.Context, payloadStr string) error {
	if j.ExecuteFunc != nil {
		return j.ExecuteFunc()
	}
	return nil
}

func caller() string {
	pcs := [13]uintptr{}
	length := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:length])
	frame, _ := frames.Next()
	return fmt.Sprintf("%s:%d", frame.Function, frame.Line)
}

var (
	errTestJob = errors.New("test job execution failed")
	cfg        = Config{
		WorkerQueueURL:     "http://localhost.localstack.cloud:4566/000000000000/worker-queue",
		DeadLetterQueueURL: "http://localhost.localstack.cloud:4566/000000000000/dead-letter-queue",
	}
)

func TestConsumer_Process(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name           string
		arrangeMessage func(msg *message.Message)
		arrange        func(client *mock_sqs.MockClient)
		assert         func(t *testing.T, got Output)
	}{
		{
			name: "successful processing",
			arrange: func(client *mock_sqs.MockClient) {
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.NoError(t, got.Error)
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to unmarshal message and sent to DLQ successfully",
			arrangeMessage: func(msg *message.Message) {
				msg.Type = ""
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
			},
			assert: func(t *testing.T, got Output) {
				assert.Empty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to unmarshal message; sent to DLQ successfully: failed to validate message")
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to unmarshal message and send to DLQ",
			arrangeMessage: func(msg *message.Message) {
				msg.Type = ""
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
			},
			assert: func(t *testing.T, got Output) {
				assert.Empty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to unmarshal message and send to DLQ: failed to enqueue message on sending to DLQ")
				assert.Equal(t, true, got.Fatal)
				assert.Equal(t, false, got.ShouldDelete)
			},
		},
		{
			name: "failed to get job and sent to DLQ successfully",
			arrangeMessage: func(msg *message.Message) {
				msg.Type = "NonExistingJob"
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to get job; sent to DLQ successfully")
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to get job and send to DLQ",
			arrangeMessage: func(msg *message.Message) {
				msg.Type = "NonExisting"
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to get job and send to DLQ")
				assert.Equal(t, true, got.Fatal)
				assert.Equal(t, false, got.ShouldDelete)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			msg := message.Message{
				ID:        uuid.New(),
				Type:      string(testJobType),
				Status:    message.Queued,
				Caller:    caller(),
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			if tc.arrangeMessage != nil {
				tc.arrangeMessage(&msg)
			}

			sqsMock := mock_sqs.NewMockClient(ctrl)
			tc.arrange(sqsMock)

			bytes, err := json.Marshal(msg)
			assert.NoError(t, err)

			// act
			sut, err := newConsumer(cfg, sqsMock, nil, getJob)
			assert.NoError(t, err)
			got := sut.Process(t.Context(), string(bytes))

			// assert
			tc.assert(t, got)
		})
	}
}

func TestConsumer_ProcessMessage_skipsWhenStatusProcessingInRedis(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	mr := miniredis.RunT(t)
	redisURL := fmt.Sprintf("redis://%s", mr.Addr())

	rds, err := redis.New(redis.Config{URL: redisURL})
	require.NoError(t, err)

	msg := message.Message{
		ID:        uuid.New(),
		Type:      string(testJobType),
		Status:    message.Queued,
		Caller:    caller(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	require.NoError(t, rds.SetMessage(ctx, msg))
	require.NoError(t, rds.UpdateMessage(ctx, msg.Processing()))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sqsMock := mock_sqs.NewMockClient(ctrl)

	cfgWithRedis := Config{
		WorkerQueueURL:     cfg.WorkerQueueURL,
		DeadLetterQueueURL: cfg.DeadLetterQueueURL,
		RedisURL:           redisURL,
	}

	sut, err := newConsumer(cfgWithRedis, sqsMock, nil, getJob)
	require.NoError(t, err)

	got := sut.ProcessMessage(ctx, msg)

	assert.False(t, got.Fatal)
	assert.ErrorIs(t, got.Error, ErrMessageAlreadyProcessing)
	assert.Equal(t, uuid.Nil, got.Message.ID)
	assert.False(t, got.ShouldDelete)
}

func TestConsumer_ProcessMessage_skipsWhenStatusSuccessInRedis(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	mr := miniredis.RunT(t)
	redisURL := fmt.Sprintf("redis://%s", mr.Addr())

	rds, err := redis.New(redis.Config{URL: redisURL})
	require.NoError(t, err)

	msg := message.Message{
		ID:        uuid.New(),
		Type:      string(testJobType),
		Status:    message.Queued,
		Caller:    caller(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	require.NoError(t, rds.SetMessage(ctx, msg))
	processing := msg.Processing()
	require.NoError(t, rds.UpdateMessage(ctx, processing))
	require.NoError(t, rds.UpdateMessage(ctx, processing.Success()))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sqsMock := mock_sqs.NewMockClient(ctrl)

	cfgWithRedis := Config{
		WorkerQueueURL:     cfg.WorkerQueueURL,
		DeadLetterQueueURL: cfg.DeadLetterQueueURL,
		RedisURL:           redisURL,
	}

	getJobFunc := func(string) (job.Job, error) {
		t.Fatalf("job execution should not be invoked for already processed messages")
		return nil, nil
	}

	sut, err := newConsumer(cfgWithRedis, sqsMock, nil, getJobFunc)
	require.NoError(t, err)

	got := sut.ProcessMessage(ctx, msg)

	assert.False(t, got.Fatal)
	assert.ErrorContains(t, got.Error, "message already processed")
	assert.Equal(t, uuid.Nil, got.Message.ID)
	assert.True(t, got.ShouldDelete)
}

func TestConsumer_doRetry_usesSchedulerWhenDelayExceedsLimit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sqsMock := mock_sqs.NewMockClient(ctrl)

	spy := &schedulerSpy{}
	cfgWithScheduler := Config{
		WorkerQueueURL:     cfg.WorkerQueueURL,
		WorkerQueueARN:     "arn:aws:sqs:us-east-1:000000000000:worker-queue",
		SchedulerRoleARN:   "arn:aws:iam::000000000000:role/scheduler-role",
		SchedulerTimeZone:  "UTC",
		MaxDelay:           3600,
		BaseDelay:          cfg.BaseDelay,
		DeadLetterQueueURL: cfg.DeadLetterQueueURL,
	}

	consumer, err := newConsumer(cfgWithScheduler, sqsMock, spy, getJob)
	assert.NoError(t, err)

	msg := message.Message{
		ID:         uuid.New(),
		Type:       string(testJobType),
		Status:     message.Retrying,
		RetryCount: 6,
		Caller:     caller(),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	expectedDelay := consumer.calculateBackoff(msg.RetryCount)
	assert.Greater(t, expectedDelay, maxSQSDelaySeconds)

	now := time.Now()
	err = consumer.doRetry(t.Context(), msg)
	assert.NoError(t, err)
	assert.True(t, spy.called)
	assert.Equal(t, fmt.Sprintf("%s-retry-%d", msg.ID.String(), msg.RetryCount), spy.scheduleName)
	assert.Equal(t, msg, spy.message)
	expectedRunAt := now.Add(time.Duration(expectedDelay) * time.Second)
	assert.WithinDuration(t, expectedRunAt, spy.runAt, 2*time.Second)
}

func TestConsumer_doRetry_returnsErrorWithoutSchedulerForLongDelay(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sqsMock := mock_sqs.NewMockClient(ctrl)

	cfgWithoutScheduler := Config{
		WorkerQueueURL:     cfg.WorkerQueueURL,
		MaxDelay:           3600,
		BaseDelay:          cfg.BaseDelay,
		DeadLetterQueueURL: cfg.DeadLetterQueueURL,
	}

	consumer, err := newConsumer(cfgWithoutScheduler, sqsMock, nil, getJob)
	assert.NoError(t, err)

	msg := message.Message{
		ID:         uuid.New(),
		Type:       string(testJobType),
		Status:     message.Retrying,
		RetryCount: 6,
		Caller:     caller(),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	err = consumer.doRetry(t.Context(), msg)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "scheduler is not configured")
}

func TestConsumer_execute(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name           string
		arrangeMessage func(msg *message.Message)
		arrange        func(client *mock_sqs.MockClient) testJob
		assert         func(t *testing.T, got Output)
	}{
		{
			name: "successful execution",
			arrange: func(client *mock_sqs.MockClient) testJob {
				return testJob{
					ExecuteFunc: func() error {
						return nil
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.NoError(t, got.Error)
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to execute job and retried successfully",
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().EnqueueWithDelay(gomock.Any(), gomock.Eq(cfg.WorkerQueueURL), gomock.Any(), gomock.Any()).Return(nil)
				return testJob{
					ExecuteFunc: func() error {
						return errTestJob
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to execute job; retried successfully")
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to execute job and retry and sent to DLQ successfully",
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().EnqueueWithDelay(gomock.Any(), gomock.Eq(cfg.WorkerQueueURL), gomock.Any(), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
				return testJob{
					ExecuteFunc: func() error {
						return errTestJob
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to execute job and retry; sent to DLQ successfully")
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "failed to execute job and retry and send to DLQ",
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().EnqueueWithDelay(gomock.Any(), gomock.Eq(cfg.WorkerQueueURL), gomock.Any(), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("DLQ enqueue failed"))
				return testJob{
					ExecuteFunc: func() error {
						return errTestJob
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to execute job and retry and send to DLQ")
				assert.Equal(t, true, got.Fatal)
				assert.Equal(t, false, got.ShouldDelete)
			},
		},
		{
			name: "max retry attempts exceeded and sent to DLQ successfully",
			arrangeMessage: func(msg *message.Message) {
				msg.RetryCount = 5
			},
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
				return testJob{
					ExecuteFunc: func() error {
						return errTestJob
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "max retry attempts exceeded; sent to DLQ successfully")
				assert.Equal(t, false, got.Fatal)
				assert.Equal(t, true, got.ShouldDelete)
			},
		},
		{
			name: "max retry attempts reached and failed to send to DLQ",
			arrangeMessage: func(msg *message.Message) {
				msg.RetryCount = 5
			},
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("DLQ enqueue failed"))
				return testJob{
					ExecuteFunc: func() error {
						return errTestJob
					},
				}
			},
			assert: func(t *testing.T, got Output) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "max retry attempts reached; failed to send to DLQ")
				assert.Equal(t, true, got.Fatal)
				assert.Equal(t, false, got.ShouldDelete)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			msg := message.Message{
				ID:        uuid.New(),
				Type:      string(testJobType),
				Caller:    caller(),
				CreatedAt: time.Now(),
			}
			if tc.arrangeMessage != nil {
				tc.arrangeMessage(&msg)
			}

			sqsMock := mock_sqs.NewMockClient(ctrl)
			testJob := tc.arrange(sqsMock)

			// act
			sut, err := newConsumer(cfg, sqsMock, nil, getJob)
			assert.NoError(t, err)
			out := sut.execute(t.Context(), testJob, msg)

			// assert
			tc.assert(t, out)
		})
	}
}
