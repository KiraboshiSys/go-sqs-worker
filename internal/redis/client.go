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

type Config struct {
	URL string
	TTL time.Duration
}

type Client struct {
	cfg    Config
	client *redis.Client
}

func New(cfg Config) (*Client, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}
	if cfg.TTL == time.Duration(0) {
		cfg.TTL = time.Hour * 24
	}
	return &Client{
		cfg:    cfg,
		client: redis.NewClient(opts),
	}, nil
}

func (c Client) SetMessage(ctx context.Context, msg message.Message) error {
	key := keyOfMessage(msg)
	err := c.client.HSet(ctx, key, messageToMap(msg)).Err()
	if err != nil {
		return fmt.Errorf("failed to set message: %w", err)
	}

	if err := c.client.Expire(ctx, key, c.cfg.TTL).Err(); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}

	return nil
}

func (c Client) DeleteMessage(ctx context.Context, msg message.Message) error {
	if err := c.client.HDel(ctx, keyOfMessage(msg)).Err(); err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
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
	err := c.client.HSet(ctx, keyOfMessage(msg), "status", msg.Status.String()).Err()
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}
	return nil
}

func messageToMap(msg message.Message) map[string]string {
	return map[string]string{
		"type":        msg.Type,
		"payload":     msg.Payload,
		"retry_count": strconv.Itoa(msg.RetryCount),
		"caller":      msg.Caller,
		"created_at":  msg.CreatedAt.Format(time.RFC3339),
		"updated_at":  msg.UpdatedAt.Format(time.RFC3339),
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

	if v, ok := m["retry_count"]; ok {
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

	if v, ok := m["created_at"]; ok {
		at, err := time.Parse(timeLayout, v)
		if err != nil {
			return msg, fmt.Errorf("failed to parse createdAt: %w", err)
		}
		msg.CreatedAt = at
	} else {
		return msg, fmt.Errorf("createdAt is required")
	}

	if v, ok := m["updated_at"]; ok {
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
