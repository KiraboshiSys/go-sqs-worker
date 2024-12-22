package consumer

import (
	"github.com/mickamy/go-sqs-worker/message"
)

type Output struct {
	Message message.Message
	Error   error
	Fatal   bool
}

// FatalError returns the error if the output is fatal, otherwise nil.
// `Fatal` means the message was not processed successfully and failed to enqueue to the dead letter queue.
func (o Output) FatalError() error {
	if o.Fatal {
		return o.Error
	}
	return nil
}

// NonFatalError returns the error if the output is not fatal, otherwise nil.
// `NonFatal` means the message was not processed successfully but enqueued to the dead letter queue successfully.
func (o Output) NonFatalError() error {
	if o.Fatal {
		return nil
	}
	return o.Error
}

func (o Output) withMessage(m message.Message) Output {
	o.Message = m
	return o
}

func nonFatalOutput(err error) Output {
	return Output{Error: err}
}

func fatalOutput(err error) Output {
	return Output{Error: err, Fatal: true}
}
