package job

import (
	"context"

	"github.com/domano/kafka-jobs/event"
	"github.com/go-xorm/xorm"
	_ "github.com/mattn/go-sqlite3"
)

type EventReader interface {
	ReadEvent(context.Context) (event.Event, error)
	Close() error
}

const InitialState = "Initial"

func Add(engine *xorm.Engine, topic, consumerGroup string) {
	r := event.NewKafkaReader(topic, []string{"localhost:9092"},
		event.WithConsumerGroup(consumerGroup),
		event.WithMinBytes(10e3),
		event.WithMaxBytes(10e6))

	for {
		e, err := r.ReadEvent(context.Background())
		if err != nil {
			break
		}
		engine.Insert(Job{Payload: string(e.Value), State: InitialState})
	}

	r.Close()
}
