package consumer

import (
	"github.com/mickamy/go-sqs-worker/message"
)

// Output represents the result of processing a message.
type Output struct {
	// The processed message.
	Message message.Message
	// The error encountered during processing, if any.
	Error error
	// Indicates if the error is fatal.
	// A fatal error means the message was not processed successfully and failed to enqueue to the dead letter queue.
	Fatal bool
}

// FatalError returns the error if the output is fatal, otherwise nil.
// A fatal error means the message was not processed successfully and failed to enqueue to the dead letter queue.
func (o Output) FatalError() error {
	if o.Fatal {
		return o.Error
	}
	return nil
}

// NonFatalError returns the error if the output is not fatal, otherwise nil.
// A non-fatal error means the message was not processed successfully but was enqueued to the dead letter queue successfully.
func (o Output) NonFatalError() error {
	if o.Fatal {
		return nil
	}
	return o.Error
}

// withMessage sets the message in the output and returns the updated output.
func (o Output) withMessage(m message.Message) Output {
	o.Message = m
	return o
}

// nonFatalOutput creates an Output with a non-fatal error.
func nonFatalOutput(err error) Output {
	return Output{Error: err}
}

// fatalOutput creates an Output with a fatal error.
func fatalOutput(err error) Output {
	return Output{Error: err, Fatal: true}
}
