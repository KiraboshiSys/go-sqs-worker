package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	"github.com/mickamy/go-sqs-worker/message"
)

//go:generate mockgen -source=$GOFILE -destination=./mock_$GOPACKAGE/mock_$GOFILE -package=mock_$GOPACKAGE
type Client interface {
	EnqueueToSQS(ctx context.Context, scheduleName string, message message.Message, at time.Time) error
	Delete(ctx context.Context, scheduleName string) error
}

func New(c *scheduler.Client, queueARN, roleARN, tz string) Client {
	return &client{
		client:   c,
		queueARN: queueARN,
		roleARN:  roleARN,
		tz:       tz,
	}
}

type client struct {
	client   *scheduler.Client
	queueARN string
	roleARN  string
	tz       string
}

func (c *client) EnqueueToSQS(ctx context.Context, scheduleName string, message message.Message, at time.Time) error {
	if c.client == nil {
		return errors.New("no scheduler client provided")
	}
	if c.queueARN == "" {
		return errors.New("no queue ARN provided")
	}
	if c.roleARN == "" {
		return errors.New("no role ARN provided")
	}
	if c.tz == "" {
		return errors.New("no timezone provided")
	}

	if scheduleName == "" {
		scheduleName = message.ID.String()
	}
	atStr := at.Format("at(2006-01-02T15:04:05)")

	input, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	inputStr := string(input)

	_, err = c.client.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               &scheduleName,
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		ScheduleExpression: &atStr,
		Target: &types.Target{
			Arn:     &c.queueARN,
			RoleArn: &c.roleARN,
			Input:   &inputStr,
		},
		ScheduleExpressionTimezone: &c.tz,
	})
	if err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}

	return nil
}

func (c *client) Delete(ctx context.Context, scheduleName string) error {
	if c.client == nil {
		return errors.New("no scheduler client provided")
	}
	if scheduleName == "" {
		return errors.New("schedule name is empty")
	}

	_, err := c.client.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name: &scheduleName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}

	return nil
}
