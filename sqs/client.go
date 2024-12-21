package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Client struct {
	client *sqs.Client
}

func New(client *sqs.Client) *Client {
	return &Client{
		client: client,
	}
}

func (c *Client) Enqueue(ctx context.Context, queueURL, message string) error {
	return c.EnqueueWithDelay(ctx, queueURL, message, 0)
}

func (c *Client) EnqueueWithDelay(ctx context.Context, queueURL, message string, delaySeconds int) error {
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody:  aws.String(message),
		QueueUrl:     aws.String(queueURL),
		DelaySeconds: int32(delaySeconds),
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

type CleanUp func(context.Context) error

var (
	noop CleanUp = func(context.Context) error { return nil }
)

func (c *Client) Dequeue(ctx context.Context, queueURL string, waitTimeSeconds int) (*string, CleanUp, error) {
	output, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     int32(waitTimeSeconds),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to receive message: %w", err)
	}

	if len(output.Messages) == 0 {
		return nil, noop, nil
	}

	var cu CleanUp = func(ctx context.Context) error {
		_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: output.Messages[0].ReceiptHandle,
		})
		if err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}
		return nil
	}

	return output.Messages[0].Body, cu, nil
}
