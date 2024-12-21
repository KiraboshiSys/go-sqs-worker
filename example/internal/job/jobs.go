package job

import (
	"fmt"
	"sort"

	"github.com/mickamy/go-sqs-worker/job"
)

type Jobs struct {
	FailingJob
	FlakyJob
	SuccessfulJob
}

//go:generate stringer -type=jobType
type jobType int

const (
	first jobType = iota
	FailingJobType
	FlakyJobType
	SuccessfulJobType
	last
)

func Get(s string, jobs Jobs) (job.Job, error) {
	types := types()

	idx := sort.Search(len(types), func(i int) bool {
		return types[i].String() >= s
	})

	if idx == len(types) || types[idx].String() != s {
		return nil, fmt.Errorf("unknown job type: [%s]", s)
	}

	switch types[idx] {
	case first, last:
		return nil, fmt.Errorf("type `first` and `last` should not be used")
	case FailingJobType:
		return jobs.FailingJob, nil
	case FlakyJobType:
		return jobs.FlakyJob, nil
	case SuccessfulJobType:
		return jobs.SuccessfulJob, nil
	}

	return nil, fmt.Errorf("unknown job type: %s", s)
}

func types() []jobType {
	var types []jobType
	for i := first + 1; i < last; i++ {
		types = append(types, i)
	}
	sort.Slice(types, func(i, j int) bool {
		return types[i].String() < types[j].String()
	})
	return types
}
