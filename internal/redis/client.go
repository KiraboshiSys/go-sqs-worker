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

	"github.com/KiraboshiSys/go-sqs-worker/message"
)

const (
	_messagesKey = "gsw:messages"
	_statusesKey = "gsw:statuses"
	_locks       = "gsw:locks"
	timeLayout   = time.RFC3339
)

var (
	ErrMissingMessageID = errors.New("missing message ID")
	ErrStatusNotFound   = errors.New("status not found")
	ErrMultipleStatuses = errors.New("multiple status keys found")
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
	key := fmt.Sprintf("%s:%s", _statusesKey, id.String())

	keys, err := c.client.Keys(ctx, key+":*").Result()
	if err != nil {
		return "", fmt.Errorf("failed to get keys for pattern [%s]: %w", key+":*", err)
	}

	if len(keys) == 0 {
		return "", fmt.Errorf("%w: id=[%s]", ErrStatusNotFound, id)
	}

	if len(keys) > 1 {
		return "", fmt.Errorf("%w: id=[%s]", ErrMultipleStatuses, id)
	}

	latestKey := keys[len(keys)-1]
	parts := strings.Split(latestKey, ":")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid status key format: %s", latestKey)
	}

	return message.Status(parts[3]), nil
}

// SetMessage stores the message in Redis and updates its status.
// It ensures the operation is atomic by acquiring a lock on the key.
func (c *Client) SetMessage(ctx context.Context, msg message.Message) (err error) {
	if msg.ID == uuid.Nil {
		return ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, msg.ID.String())
	lockValue, err := c.lockKey(ctx, key, c.cfg.TTL)
	if err != nil {
		return fmt.Errorf("failed to lock key: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockKey(ctx, key, lockValue); unlockErr != nil {
			err = errors.Join(ErrUnlockFailed, fmt.Errorf("key=[%s]: %w", key, unlockErr))
		}
	}()

	_, err = c.client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		newStatusKey := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.Status.String())
		if err := pipeliner.Set(ctx, newStatusKey, "", c.cfg.TTL).Err(); err != nil {
			return fmt.Errorf("failed to set new status: %w", err)
		}

		if msg.OldStatus != "" {
			oldStatusKey := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.OldStatus.String())
			if err := pipeliner.Del(ctx, oldStatusKey).Err(); err != nil {
				return fmt.Errorf("failed to delete old status: %w", err)
			}
		}

		if err := pipeliner.HSet(ctx, key, messageToMap(msg)).Err(); err != nil {
			return fmt.Errorf("failed to set message: %w", err)
		}

		if err := pipeliner.Expire(ctx, key, c.cfg.TTL).Err(); err != nil {
			return fmt.Errorf("failed to set TTL for message: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}

	return nil
}

// UpdateMessage updates the status, retry count, and updated at of the message.
// It ensures the operation is atomic by acquiring a lock on the key.
func (c *Client) UpdateMessage(ctx context.Context, msg message.Message) (err error) {
	if msg.Status == msg.OldStatus {
		// no need to update the status
		return nil
	}

	if msg.ID == uuid.Nil {
		return ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, msg.ID.String())
	lockValue, err := c.lockKey(ctx, key, c.cfg.TTL)
	if err != nil {
		return fmt.Errorf("failed to lock key: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockKey(ctx, key, lockValue); unlockErr != nil {
			err = errors.Join(ErrUnlockFailed, fmt.Errorf("key=[%s]: %w", key, unlockErr))
		}
	}()

	_, err = c.client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		newStatusKey := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.Status.String())
		if err := pipeliner.Set(ctx, newStatusKey, "", c.cfg.TTL).Err(); err != nil {
			return fmt.Errorf("failed to set new status: %w", err)
		}

		if msg.OldStatus != "" {
			oldStatusKey := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.OldStatus.String())
			if err := pipeliner.Del(ctx, oldStatusKey).Err(); err != nil {
				return fmt.Errorf("failed to delete old status: %w", err)
			}
		}

		if err := pipeliner.HSet(ctx, key, "status", msg.Status.String(), "retry_count", msg.RetryCount, "updated_at", msg.UpdatedAt.Format(timeLayout)).Err(); err != nil {
			return fmt.Errorf("failed to update status: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}

	return nil
}

func (c *Client) lockKey(ctx context.Context, key string, ttl time.Duration) (string, error) {
	lockKey := fmt.Sprintf("%s:%s", _locks, key)
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
	lockKey := fmt.Sprintf("%s:%s", _locks, key)
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
