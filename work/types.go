package work

import (
	"context"

	"github.com/domano/kafka-jobs/event"

	"github.com/domano/kafka-jobs/job"
)

type EventReader interface {
	ReadEvent(context.Context) (event.Event, error)
	Close() error
}

type DbConnector interface {
	Insert(job.Job) error
	Update(job.Job) error
	Find(state string) ([]job.Job, error)
	Close() error
}

type Worker func(job job.Job) (newState string)
