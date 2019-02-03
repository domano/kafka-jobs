package db

import (
	"log"

	"github.com/domano/kafka-jobs/job"

	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"
	"github.com/pkg/errors"
)

type SqliteConnector struct {
	engine *xorm.Engine
}

func NewSqliteEngine(path string, beans ...interface{}) *SqliteConnector {
	engine, err := xorm.NewEngine("sqlite3", path)
	if err != nil {
		log.Fatalf("Could not init xorm engine: %v", err)
	}
	engine.SetMapper(core.GonicMapper{})
	engine.Sync2(beans)

	return &SqliteConnector{engine}
}

func (c *SqliteConnector) Update(job job.Job) error {
	if _, err := c.engine.Id(job.Id).Update(job); err != nil {
		return errors.Wrap(err, "Could not update via sqlite connector: ")
	}
	return nil
}

func (c *SqliteConnector) Insert(job job.Job) error {
	if _, err := c.engine.Insert(job); err != nil {
		return errors.Wrap(err, "Could not insert via sqlite connector: ")
	}
	return nil
}

func (c *SqliteConnector) Find(state string) ([]job.Job, error) {
	var jobs []job.Job
	if err := c.engine.Where("state = ?", state).Find(&jobs); err != nil {
		return nil, errors.Wrap(err, "Could not find jobs via sqlite connector: ")
	}
	return jobs, nil
}

func (c *SqliteConnector) Close() error {
	if err := c.engine.Close(); err != nil {
		return errors.Wrap(err, "Could not close SqliteConnector: ")
	}
	return nil
}
