package work

import (
	"log"

	"github.com/pkg/errors"
)

// TODO: Optimistic locking does not work!
func Work(db DbConnector, state string, worker Worker) error {
	jobs, err := db.Find(state)
	if err != nil {
		return errors.Wrap(err, "Could not get work: ")
	}
	for i := range jobs {
		go func() {
			log.Printf("Working on job %+v\n", jobs[i])

			// Mark the job to avoid it being processed multiple times
			newJob := jobs[i]
			newJob.State = state + " - processing"
			if err := db.Update(newJob); err != nil {
				log.Println(errors.Wrap(err, "Failed to mark job as being in progress").Error())
			}

			// Move to next state
			newState := worker(jobs[i])
			newJob.State = newState
			if err := db.Update(newJob); err != nil {
				log.Println(errors.Wrap(err, "Failed to mark job as processed").Error())
			}
		}()
	}
	return nil
}
