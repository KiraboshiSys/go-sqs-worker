package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mickamy/go-sqs-worker/message"
)

const (
	messagesKey = "messages"
	timeLayout  = time.RFC3339
)

type Client struct {
	client *redis.Client
}

func New(url string) (*Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	return &Client{
		client: redis.NewClient(opts),
	}, nil
}

func (c Client) AddMessage(ctx context.Context, msg message.Message) error {
	return c.client.HSet(ctx, keyOfMessage(msg), messageToMap(msg)).Err()
}

func (c Client) DeleteMessage(ctx context.Context, msg message.Message) error {
	return c.client.HDel(ctx, keyOfMessage(msg)).Err()
}

func (c Client) ListMessages(ctx context.Context) ([]message.Message, error) {
	vals, err := c.client.HGetAll(ctx, messagesKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get messages: %w", err)
	}

	var msgs []message.Message
	for _, val := range vals {
		msgMap := map[string]string{}
		if err := json.Unmarshal([]byte(val), &msgMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message: %w", err)
		}

		msg, err := mapToMessage(msgMap)
		if err != nil {
			return nil, fmt.Errorf("failed to convert map to message: %w", err)
		}

		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (c Client) UpdateStatus(ctx context.Context, msg message.Message) error {
	return c.client.HSet(ctx, keyOfMessage(msg), "status", msg.Status.String()).Err()
}

func messageToMap(msg message.Message) map[string]string {
	return map[string]string{
		"type":       msg.Type,
		"payload":    msg.Payload,
		"retryCount": strconv.Itoa(msg.RetryCount),
		"caller":     msg.Caller,
		"createdAt":  msg.CreatedAt.Format(time.RFC3339),
		"updatedAt":  msg.UpdatedAt.Format(time.RFC3339),
	}
}

func mapToMessage(m map[string]string) (message.Message, error) {
	msg := message.Message{}

	if v, ok := m["type"]; ok {
		msg.Type = v
	} else {
		return msg, fmt.Errorf("type is required")
	}

	if v, ok := m["payload"]; ok {
		msg.Payload = v
	} else {
		return msg, fmt.Errorf("payload is required")
	}

	if v, ok := m["retryCount"]; ok {
		cnt, err := strconv.Atoi(v)
		if err != nil {
			return msg, fmt.Errorf("failed to convert retryCount: %w", err)
		}
		msg.RetryCount = cnt
	} else {
		return msg, fmt.Errorf("retryCount is required")
	}

	if v, ok := m["caller"]; ok {
		msg.Caller = v
	} else {
		return msg, fmt.Errorf("caller is required")
	}

	if v, ok := m["createdAt"]; ok {
		at, err := time.Parse(timeLayout, v)
		if err != nil {
			return msg, fmt.Errorf("failed to parse createdAt: %w", err)
		}
		msg.CreatedAt = at
	} else {
		return msg, fmt.Errorf("createdAt is required")
	}

	if v, ok := m["updatedAt"]; ok {
		at, err := time.Parse(timeLayout, v)
		if err != nil {
			return msg, fmt.Errorf("failed to parse updatedAt: %w", err)
		}
		msg.UpdatedAt = at
	} else {
		return msg, fmt.Errorf("updatedAt is required")
	}

	return msg, nil
}

func keyOfMessage(msg message.Message) string {
	return keyOfMessageID(msg.ID.String())
}

func keyOfMessageID(id string) string {
	return messagesKey + ":" + id
}
