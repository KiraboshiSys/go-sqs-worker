package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/mickamy/go-sqs-worker/message"
)

const (
	_messagesKey = "gsw:messages"
	_statusesKey = "gsw:statuses"
	_lockKey     = "gsw:lock"
	timeLayout   = time.RFC3339
)

var (
	ErrMissingMessageID = errors.New("missing message ID")
	ErrStatusNotFound   = errors.New("status not found")
	ErrLockHeld         = errors.New("lock already held")
	ErrUnlockFailed     = errors.New("failed to release lock")
)

type Config struct {
	URL     string
	TTL     time.Duration
	LockTTL time.Duration
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
	if cfg.LockTTL == time.Duration(0) {
		cfg.LockTTL = time.Second * 5
	}
	return &Client{
		cfg:    cfg,
		client: redis.NewClient(opts),
	}, nil
}

func (c *Client) GetStatus(ctx context.Context, id uuid.UUID) (message.Status, error) {
	pattern := fmt.Sprintf("%s:%s:*", _statusesKey, id.String())
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		key := strings.Split(iter.Val(), ":")
		if len(key) != 4 {
			return "", fmt.Errorf("invalid status key: %s", iter.Val())
		}
		return message.Status(key[3]), nil
	}
	if err := iter.Err(); err != nil {
		return "", fmt.Errorf("failed to scan for keys: %w", err)
	}

	return "", errors.Join(ErrStatusNotFound, fmt.Errorf("id=[%s]", id))
}

// SetMessage stores the message in Redis and updates its status.
// It ensures the operation is atomic by acquiring a lock on the key.
func (c *Client) SetMessage(ctx context.Context, msg message.Message) (err error) {
	if msg.ID == uuid.Nil {
		return ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, msg.ID.String())

	// acquire lock
	lockValue, err := c.lockKey(ctx, key, c.cfg.TTL)
	if err != nil {
		return fmt.Errorf("failed to lock key: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockKey(ctx, key, lockValue); unlockErr != nil {
			err = errors.Join(ErrUnlockFailed, fmt.Errorf("key=[%s]: %v", key, unlockErr))
		}
	}()

	err = c.client.HSet(ctx, key, messageToMap(msg)).Err()
	if err != nil {
		return fmt.Errorf("failed to set message: %w", err)
	}
	if err := c.setTTL(ctx, key); err != nil {
		return err
	}

	return c.updateStatus(ctx, msg)
}

func (c *Client) updateStatus(ctx context.Context, msg message.Message) error {
	pattern := fmt.Sprintf("%s:%s:*", _statusesKey, msg.ID.String())
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()

	pipeline := c.client.Pipeline()
	for iter.Next(ctx) {
		pipeline.Del(ctx, iter.Val())
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan for keys: %w", err)
	}

	key := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.Status.String())
	if err := c.client.Set(ctx, key, "", c.cfg.TTL).Err(); err != nil {
		return fmt.Errorf("failed to set status: %w", err)
	}

	return nil
}

func (c *Client) setTTL(ctx context.Context, key string) error {
	if err := c.client.Expire(ctx, key, c.cfg.TTL).Err(); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}
	return nil
}

func (c *Client) lockKey(ctx context.Context, key string, ttl time.Duration) (string, error) {
	lockKey := fmt.Sprintf("%s:%s", _lockKey, key)
	lockValue := uuid.New().String() // unique identifier for the lock

	// attempt to acquire the lock
	result, err := c.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
	if err != nil {
		return "", fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !result {
		return "", errors.Join(ErrLockHeld, fmt.Errorf("key=[%s]", key))
	}

	return lockValue, nil
}

func (c *Client) unlockKey(ctx context.Context, key, lockValue string) error {
	lockKey := fmt.Sprintf("%s:%s", _lockKey, key)
	currentValue, err := c.client.Get(ctx, lockKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get lock value: %w", err)
	}
	if currentValue != lockValue {
		return fmt.Errorf("lock value mismatch for key: %s", key)
	}

	if err := c.client.Del(ctx, lockKey).Err(); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
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
		"created_at":  msg.CreatedAt.Format(timeLayout),
		"updated_at":  msg.UpdatedAt.Format(timeLayout),
	}
}
