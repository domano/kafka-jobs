package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/domano/kafka-jobs/db"
	"github.com/domano/kafka-jobs/event"
	"github.com/domano/kafka-jobs/job"
	"github.com/domano/kafka-jobs/work"

	"github.com/segmentio/kafka-go"
)

func main() {
	db := db.NewSqliteEngine("./test.db")

	topic := "jobs"

	r := event.NewKafkaReader(topic, []string{"localhost:9092"},
		event.WithConsumerGroup("testgroup"),
		event.WithMinBytes(10e3),
		event.WithMaxBytes(10e6))

	fetcher := work.NewFetcher(db, work.WithEventReader(&r))

	go testWrite(topic)
	fetcher.Fetch()

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
	for {
		// to produce messages
		partition := 0

		conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

		p := make([]byte, rand.Int()%64)
		rand.Read(p)
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		conn.WriteMessages(
			kafka.Message{Value: p},
		)

		conn.Close()
		<-time.After(1 * time.Second)
	}
}

func testWorker(job job.Job) string {
	<-time.After(1 * time.Second)
	return "Processed"
}
