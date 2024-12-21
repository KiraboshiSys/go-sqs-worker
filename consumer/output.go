package consumer

import (
	"github.com/mickamy/go-sqs-worker/worker"
)

type JobProcessingOutput struct {
	Message worker.Message
	Error   error
	Fatal   bool
}

func (o JobProcessingOutput) FatalError() error {
	if o.Fatal {
		return o.Error
	}
	return nil
}

func (o JobProcessingOutput) NonFatalError() error {
	if o.Fatal {
		return nil
	}
	return o.Error
}

func nonFatalJobProcessingOutput(err error) JobProcessingOutput {
	return JobProcessingOutput{Error: err}
}

func fatalJobProcessingOutput(err error) JobProcessingOutput {
	return JobProcessingOutput{Error: err, Fatal: true}
}
