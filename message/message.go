/*
Package message provides structures and functions for handling messages in the job processing system.

The Message struct represents a job message with fields for ID, type, payload, retry count, caller information, and creation time.
The package also includes functions for creating new messages, retrying messages, and setting caller information.

Types:

  - Message: Represents a job message with fields for ID, type, payload, retry count, caller information, and creation time.
  - New: Creates a new Message with the given type and payload, and validates the payload.

Functions:

  - Message.Retry: Increments the RetryCount of the Message.
  - Message.SetCaller: Sets the Caller field of the Message.
  - New: Creates a new Message with the given type and payload, and validates the payload.

Usage:

To create a new message, use the New function:

	msg, err := message.NewMessage(ctx, "jobType", payload)
	if err != nil {
	    // handle error
	}

To retry a message, use the Message.Retry method:

	msg.Retry()

To set the caller information of a message, use the Message.SetCaller method:

	msg.SetCaller("callerInfo")
*/
package message

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

var (
	validate = validator.New()
)

// Message represents a job message.
// The Message struct is used to pass job information between components of the job processing system.
type Message struct {
	// ID is the unique identifier of the job
	ID uuid.UUID `json:"id" validate:"required,uuid"`

	// Type is the type of the job. It is used to determine the job handler
	Type string `json:"type" validate:"required"`

	// Payload is the data to be passed to the job
	Payload string `json:"payload" validate:"-"`

	// RetryCount is the number of times the job has been retried
	RetryCount int `json:"retry_count" validate:"-"`

	// Caller is the information about the caller of the job
	Caller string `json:"caller" validate:"required"`

	// CreatedAt is the time the job was created
	CreatedAt time.Time `json:"created_at" validate:"required"`
}

// Retry increments the RetryCount of the job
func (m *Message) Retry() {
	m.RetryCount++
}

// SetCaller sets the Caller of the job
func (m *Message) SetCaller(caller string) {
	m.Caller = caller
}

// New creates a new Message
func New(ctx context.Context, jobType string, payload any) (Message, error) {
	id := uuid.New()
	bytes, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal payload: %w", err)
	}
	if err := validate.StructCtx(ctx, payload); err != nil {
		return Message{}, fmt.Errorf("validation failed: %v", err)
	}
	return Message{
		ID:        id,
		Type:      jobType,
		Payload:   string(bytes),
		CreatedAt: time.Now(),
	}, nil
}
