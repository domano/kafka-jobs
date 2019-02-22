package work

import (
	"context"

	"github.com/domano/kafka-jobs/job"
	_ "github.com/mattn/go-sqlite3"
)

const InitialState = "Initial"

type Fetcher struct {
	db  DbConnector
	ers []EventReader
}

type FetcherOption func(*Fetcher)

func NewFetcher(db DbConnector, options ...FetcherOption) *Fetcher {
	f := Fetcher{db: db}
	for _, option := range options {
		option(&f)
	}
	return &f
}

func WithEventReader(er EventReader) FetcherOption {
	return func(f *Fetcher) {
		f.ers = append(f.ers, er)
	}
}

func (f *Fetcher) Fetch() {
	for i := range f.ers {
		go fetch(f.ers[i], f.db)
	}
}

func fetch(er EventReader, db DbConnector) {
	for {
		e, err := er.ReadEvent(context.Background())
		if err != nil {
			break
		}
		db.Insert(job.Job{Payload: string(e.Value), State: InitialState})
	}
	er.Close()
}
