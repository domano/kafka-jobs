package job

import (
	"log"

	"github.com/go-xorm/xorm"
)

type Worker func(job Job) (newState string)

func Work(engine *xorm.Engine, state string, worker Worker) {
	var jobs []Job
	engine.Where("state = ?", state).Find(&jobs)
	for i := range jobs {

		log.Printf("Working on job %+v\n", jobs[i])

		// Mark the job to avoid it being processed multiple times
		newJob := jobs[i]
		newJob.State = state + " - processing"
		engine.Id(newJob.Id).Update(&newJob)

		// Move to next state
		newState := worker(jobs[i])
		newJob.State = newState
		engine.Id(newJob.Id).Update(&newJob)
	}
}
