package consumer

import (
	"github.com/mickamy/go-sqs-worker/worker"
)

type Output struct {
	Message worker.Message
	Error   error
	Fatal   bool
}

func (o Output) FatalError() error {
	if o.Fatal {
		return o.Error
	}
	return nil
}

func (o Output) NonFatalError() error {
	if o.Fatal {
		return nil
	}
	return o.Error
}

func (o Output) WithMessage(m worker.Message) Output {
	return Output{
		Message: m,
		Error:   o.Error,
		Fatal:   o.Fatal,
	}
}

func nonFatalOutput(err error) Output {
	return Output{Error: err}
}

func fatalOutput(err error) Output {
	return Output{Error: err, Fatal: true}
}
