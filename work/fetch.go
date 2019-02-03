package work

import (
	"context"

	"github.com/domano/kafka-jobs/event"
	"github.com/domano/kafka-jobs/job"
	_ "github.com/mattn/go-sqlite3"
)

const InitialState = "Initial"

func Fetch(db DbConnector, topic, consumerGroup string) {
	r := event.NewKafkaReader(topic, []string{"localhost:9092"},
		event.WithConsumerGroup(consumerGroup),
		event.WithMinBytes(10e3),
		event.WithMaxBytes(10e6))

	for {
		e, err := r.ReadEvent(context.Background())
		if err != nil {
			break
		}
		db.Insert(job.Job{Payload: string(e.Value), State: InitialState})
	}

	r.Close()
}
