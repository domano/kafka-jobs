package main

import (
	"context"
	"time"

	"github.com/domano/kafka-jobs/db"
	"github.com/domano/kafka-jobs/job"
	"github.com/domano/kafka-jobs/work"

	"github.com/segmentio/kafka-go"
)

func main() {
	db := db.NewSqliteEngine("./test.db", &job.Job{})

	topic := "jobs"
	go testWrite(topic)
	go work.Fetch(db, topic, "testgroup")

	tick := time.Tick(time.Second)

	go func() {
		for {
			select {
			case <-tick:
				work.Work(db, work.InitialState, testWorker)
			}
		}
	}()

	select {}
}

func testWrite(topic string) {
	// to produce messages
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)

	conn.Close()
}

func testWorker(job job.Job) string {
	<-time.After(10 * time.Second)
	return "Processed"
}
