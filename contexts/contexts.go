package contexts

import (
	"context"
	"errors"
)

type messageIDKey struct{}

// SetMessageID sets the message ID in the context.
func SetMessageID(ctx context.Context, messageID string) context.Context {
	return context.WithValue(ctx, messageIDKey{}, messageID)
}

// MessageID retrieves the message ID from the context.
func MessageID(ctx context.Context) (string, error) {
	messageID, ok := ctx.Value(messageIDKey{}).(string)
	if ok {
		return messageID, nil
	}
	return "", errors.New("no message ID found in context")
}

// MustMessageID retrieves the message ID from the context and panics if not found.
func MustMessageID(ctx context.Context) string {
	messageID, err := MessageID(ctx)
	if err != nil {
		panic(err)
	}
	return messageID
}
