/*
Package message provides structures and functions for handling messages in the job processing system.

The Message struct represents a job message with fields for ID, type, payload, retry count, caller information, and creation time.
The package also includes functions for creating new messages, retrying messages, and setting caller information.

Types:

  - Message: Represents a job message with fields for ID, type, payload, retry count, caller information, and creation time.
  - New: Creates a new Message with the given type and payload, and validates the payload.

Functions:

  - Message.SetCaller: Sets the Caller field of the Message.
  - Message.SetStatus: Sets the Status field of the Message.
  - Message.Processing: Returns a new Message with the status set to Processing.
  - Message.Retrying: Returns a new Message with the status set to Retrying.
  - Message.Success: Returns a new Message with the status set to Success.
  - Message.Failed: Returns a new Message with the status set to Failed.
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

type Status string

const (
	Queued     Status = "queued"
	Processing Status = "processing"
	Retrying   Status = "retrying"
	Success    Status = "success"
	Failed     Status = "failed"
)

func (s Status) String() string {
	return string(s)
}

// ShouldProcess returns true if the status is Queued or Retrying
func (s Status) ShouldProcess() bool {
	return s == Queued || s == Retrying
}

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

	// Status is the current status of the job
	Status Status `json:"status" validate:"required"`

	// RetryCount is the number of times the job has been retried
	RetryCount int `json:"retry_count" validate:"-"`

	// Caller is the information about the caller of the job
	Caller string `json:"caller" validate:"required"`

	// CreatedAt is the time the job was created
	CreatedAt time.Time `json:"created_at" validate:"required"`

	// UpdatedAt is the time the job was last updated
	UpdatedAt time.Time `json:"updated_at" validate:"required"`
}

// SetCaller sets the Caller of the job
func (m *Message) SetCaller(caller string) {
	m.Caller = caller
	m.UpdatedAt = time.Now()
}

// SetStatus sets the Status of the job
func (m *Message) SetStatus(status Status) {
	m.Status = status
	m.UpdatedAt = time.Now()
}

func (m *Message) Processing() Message {
	return Message{
		ID:         m.ID,
		Type:       m.Type,
		Payload:    m.Payload,
		Status:     Processing,
		RetryCount: m.RetryCount,
		Caller:     m.Caller,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  time.Now(),
	}
}

func (m *Message) Retrying() Message {
	return Message{
		ID:         m.ID,
		Type:       m.Type,
		Payload:    m.Payload,
		Status:     Retrying,
		RetryCount: m.RetryCount + 1,
		Caller:     m.Caller,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  time.Now(),
	}
}

func (m *Message) Success() Message {
	return Message{
		ID:         m.ID,
		Type:       m.Type,
		Payload:    m.Payload,
		Status:     Success,
		RetryCount: m.RetryCount,
		Caller:     m.Caller,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  time.Now(),
	}
}

func (m *Message) Failed() Message {
	return Message{
		ID:         m.ID,
		Type:       m.Type,
		Payload:    m.Payload,
		Status:     Failed,
		RetryCount: m.RetryCount,
		Caller:     m.Caller,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  time.Now(),
	}
}

// New creates a new Message
func New(ctx context.Context, jobType string, payload any) (Message, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("failed to marshal payload: %w", err)
	}
	if validationErr := validate.StructCtx(ctx, payload); validationErr != nil {
		return Message{}, fmt.Errorf("validation failed: %w", validationErr)
	}
	return Message{
		ID:         uuid.New(),
		Type:       jobType,
		Payload:    string(bytes),
		Status:     Queued,
		RetryCount: 0,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}, nil
}
