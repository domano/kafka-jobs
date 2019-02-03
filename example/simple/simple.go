package main

import (
	"context"
	"log"
	"time"

	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"

	"github.com/domano/kafka-jobs/job"

	"github.com/segmentio/kafka-go"
)

func main() {
	engine, err := xorm.NewEngine("sqlite3", "./test.db")
	if err != nil {
		log.Fatalf("Could not init xorm engine: %v", err)
	}
	engine.SetMapper(core.GonicMapper{})
	engine.Sync2(new(job.Job))

	topic := "jobs"
	go testWrite(topic)
	go job.Add(engine, topic, "testgroup")

	tick := time.Tick(time.Second)

	go func() {
		for {
			select {
			case <-tick:
				job.Work(engine, job.InitialState, testWorker)
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
