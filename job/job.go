package job

import "time"

type Job struct {
	Id      int64
	Key     string `xorm:"index"`
	Payload string `xorm:"text"`
	State   string
	Created time.Time `xorm:"created"`
	Updated time.Time `xorm:"updated"`
}
