package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mickamy/go-sqs-worker/message"
)

const (
	messagesKey = "gsw:messages"
	statusesKey = "gsw:statuses"
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
	key := fmt.Sprintf("%s:%s", messagesKey, msg.ID)
	err := c.client.HSet(ctx, key, messageToMap(msg)).Err()
	if err != nil {
		return fmt.Errorf("failed to set message: %w", err)
	}
	if err := c.setTTL(ctx, key); err != nil {
		return err
	}

	return c.addStatus(ctx, msg)
}

func (c Client) addStatus(ctx context.Context, msg message.Message) error {
	pattern := fmt.Sprintf("%s:%s:*", statusesKey, msg.ID)
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		if err := c.client.Del(ctx, iter.Val()).Err(); err != nil {
			return fmt.Errorf("failed to delete old status: %w", err)
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan for keys: %w", err)
	}

	key := fmt.Sprintf("%s:%s:%s", statusesKey, msg.ID, msg.Status.String())
	value := "" // value may be empty because we only need the key
	if err := c.client.Set(ctx, key, value, c.cfg.TTL).Err(); err != nil {
		return fmt.Errorf("failed to set status: %w", err)
	}

	return nil
}

func (c Client) setTTL(ctx context.Context, key string) error {
	if err := c.client.Expire(ctx, key, c.cfg.TTL).Err(); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}
	return nil
}

func messageToMap(msg message.Message) map[string]string {
	return map[string]string{
		"type":        msg.Type,
		"payload":     msg.Payload,
		"status":      msg.Status.String(),
		"retry_count": strconv.Itoa(msg.RetryCount),
		"caller":      msg.Caller,
		"created_at":  msg.CreatedAt.Format(time.RFC3339),
		"updated_at":  msg.UpdatedAt.Format(time.RFC3339),
	}
}
