package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
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
	_locks       = "gsw:locks"
	timeLayout   = time.RFC3339
)

var (
	ErrMissingMessageID = errors.New("missing message ID")
	ErrStatusNotFound   = errors.New("status not found")
	ErrMultipleStatuses = errors.New("multiple status keys found")
	ErrStatusConflict   = errors.New("status conflict")
	ErrLockHeld         = errors.New("lock already held")
	ErrUnlockFailed     = errors.New("failed to release lock")
)

type Config struct {
	URL     string
	LockTTL time.Duration
}

type Client struct {
	cfg    Config
	client *redis.Client
}

func parseURL(rawURL string) (*redis.Options, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" && u.Scheme != "valkey" {
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "6379"
	}

	db := 0
	if strings.TrimPrefix(u.Path, "/") != "" {
		db, err = strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
		if err != nil {
			return nil, fmt.Errorf("invalid db number: %w", err)
		}
	}

	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Username: username,
		Password: password,
		DB:       db,
	}

	if u.Scheme == "rediss" {
		opts.TLSConfig = &tls.Config{}
	}

	return opts, nil
}

func New(cfg Config) (*Client, error) {
	opts, err := parseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
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
	lockValue, err := c.lockKey(ctx, key, c.cfg.LockTTL)
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
		if err := pipeliner.Set(ctx, newStatusKey, "", 0).Err(); err != nil {
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
	if msg.OldStatus == "" {
		return fmt.Errorf("old status is required for update: id=[%s]", msg.ID)
	}

	if msg.ID == uuid.Nil {
		return ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, msg.ID.String())
	lockValue, err := c.lockKey(ctx, key, c.cfg.LockTTL)
	if err != nil {
		return fmt.Errorf("failed to lock key: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockKey(ctx, key, lockValue); unlockErr != nil {
			err = errors.Join(ErrUnlockFailed, fmt.Errorf("key=[%s]: %w", key, unlockErr))
		}
	}()

	currentStatus, err := c.client.HGet(ctx, key, "status").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return fmt.Errorf("%w: id=[%s]", ErrStatusNotFound, msg.ID)
		}
		return fmt.Errorf("failed to get current status: %w", err)
	}
	if currentStatus != msg.OldStatus.String() {
		return fmt.Errorf("%w: expected=[%s] actual=[%s]", ErrStatusConflict, msg.OldStatus, currentStatus)
	}

	_, err = c.client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		newStatusKey := fmt.Sprintf("%s:%s:%s", _statusesKey, msg.ID.String(), msg.Status.String())
		if err := pipeliner.Set(ctx, newStatusKey, "", 0).Err(); err != nil {
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

// DeleteMessage deletes the message and its status from Redis.
// It ensures the operation is atomic by acquiring a lock on the key.
func (c *Client) DeleteMessage(ctx context.Context, id uuid.UUID) (err error) {
	if id == uuid.Nil {
		return ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, id.String())
	lockValue, err := c.lockKey(ctx, key, c.cfg.LockTTL)
	if err != nil {
		return fmt.Errorf("failed to lock key: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockKey(ctx, key, lockValue); unlockErr != nil {
			err = errors.Join(ErrUnlockFailed, fmt.Errorf("key=[%s]: %w", key, unlockErr))
		}
	}()

	// get all status keys for the message
	statusPattern := fmt.Sprintf("%s:%s:*", _statusesKey, id.String())
	statusKeys, err := c.client.Keys(ctx, statusPattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get status keys: %w", err)
	}

	_, err = c.client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		// delete message key
		if err := pipeliner.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}

		// delete all status keys
		if len(statusKeys) > 0 {
			if err := pipeliner.Del(ctx, statusKeys...).Err(); err != nil {
				return fmt.Errorf("failed to delete status keys: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}

	return nil
}

func (c *Client) ListMessageIDs(ctx context.Context, status message.Status) ([]uuid.UUID, error) {
	pattern := fmt.Sprintf("%s:*:%s", _statusesKey, status.String())
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys for pattern [%s]: %w", pattern, err)
	}

	var ids []uuid.UUID
	for _, key := range keys {
		parts := strings.Split(key, ":")
		if len(parts) < 3 {
			continue // skip invalid keys
		}
		idStr := parts[2]
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue // skip invalid UUIDs
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (c *Client) GetMessage(ctx context.Context, id uuid.UUID) (message.Message, error) {
	if id == uuid.Nil {
		return message.Message{}, ErrMissingMessageID
	}
	key := fmt.Sprintf("%s:%s", _messagesKey, id.String())

	data, err := c.client.HGetAll(ctx, key).Result()
	if err != nil {
		return message.Message{}, fmt.Errorf("failed to get message: %w", err)
	}
	if len(data) == 0 {
		return message.Message{}, fmt.Errorf("message not found: id=[%s]", id)
	}

	retryCount, err := strconv.Atoi(data["retry_count"])
	if err != nil {
		return message.Message{}, fmt.Errorf("invalid retry_count: %w", err)
	}

	createdAt, err := time.Parse(timeLayout, data["created_at"])
	if err != nil {
		return message.Message{}, fmt.Errorf("invalid created_at: %w", err)
	}

	updatedAt, err := time.Parse(timeLayout, data["updated_at"])
	if err != nil {
		return message.Message{}, fmt.Errorf("invalid updated_at: %w", err)
	}

	return message.Message{
		ID:         id,
		Type:       data["type"],
		Payload:    data["payload"],
		Status:     message.Status(data["status"]),
		RetryCount: retryCount,
		Caller:     data["caller"],
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
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
