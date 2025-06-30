package async_jobs

import (
	"fmt"
	"time"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/go-co-op/gocron"
)

type JobManager interface {
	scheduleJob(cronExpression string, id string, task interface{}) (*gocron.Job, error)
	unScheduleJob(id string) error
	isJobScheduled(id string) bool
	RequestJobMessageQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error)
	GetJobState(brokerid string, instanceid string, operation base.Operation) (*AsyncJobState, error)
}

// can be called to initialize the manager
// defaults to do clean-up of jobstates after an hour.
// runs clean up check every 15 minutes
func NewAsyncJobManager() *AsyncJobManager {
	return &AsyncJobManager{
		jobStates:    make(map[AsyncJobKey]AsyncJobState),
		brokerQueues: make(map[AsyncJobKey]chan AsyncJobMsg),
		cleanup:      make(map[AsyncJobKey]time.Time),
		scheduler:    gocron.NewScheduler(time.Local),
		expiration:   5 * time.Minute, //platform issues last-operation calls every 2 minutes
		check:        2 * time.Minute,
	}
}

// must be called to activate cleanup mechanism
// separated from constructor to allow config and testing
func (q *AsyncJobManager) Init() {
	q.scheduler.TagsUnique()
	q.scheduler.Every(q.check).Tag("QueueCleaner").Do(q.cleanupJobStates)
	q.scheduler.StartAsync()
}

// Allow Jobs to be scheduled by brokers
func (q *AsyncJobManager) scheduleJob(cronExpression string, id string, task interface{}) (*gocron.Job, error) {
	return q.scheduler.Cron(cronExpression).Tag(id).Do(task)
}

// Stop jobs scheduled
func (q *AsyncJobManager) unscheduleJob(id string) error {
	return q.scheduler.RemoveByTag(id)
}

// Determine if job(id) is scheduled
func (q *AsyncJobManager) isJobScheduled(id string) bool {
	for _, job := range q.scheduler.Jobs() {
		for _, tag := range job.Tags() {
			if id == tag {
				return true
			}
		}
	}
	return false
}

// update the state list for the unique job
func (q *AsyncJobManager) processMsg(msg AsyncJobMsg) {
	key := &AsyncJobKey{
		BrokerId:   msg.BrokerId,
		InstanceId: msg.InstanceId,
		Operation:  msg.JobType,
	}
	q.jobStates[*key] = msg.JobState
	if msg.ProcessedStatus != nil {
		msg.ProcessedStatus <- true
		close(msg.ProcessedStatus)
	}
}

// async job monitor will process any messages
// coming in on the channel and then update the state for that job
func (q *AsyncJobManager) msgProcessor(jobChan chan AsyncJobMsg, key *AsyncJobKey) {
	for job := range jobChan {
		q.processMsg(job)
	}
	// channel is closed so remove key from chan queue and mark state queue for cleanup
	delete(q.brokerQueues, *key)
	// schedule clean up of this job's state in the future
	q.cleanup[*key] = time.Now().Add(q.expiration)
}

// async cron job to remove expired jobstates
// we need to delay clean up so the broker has
// time to retrieve state afer a job completes
// Create,Delete clean-up can be lazy,
// TODO: Bind,Unbind,Modify operations should be cleaned quickly ?
func (q *AsyncJobManager) cleanupJobStates() {
	now := time.Now()
	for key, due := range q.cleanup {
		if now.After(due) {
			delete(q.jobStates, key)
			delete(q.cleanup, key)
		}
	}
}

func (q *AsyncJobManager) getJobMessageKey(brokerid string, instanceid string, operation base.Operation) *AsyncJobKey {
	return &AsyncJobKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
}

// a broker or adapter can request a channel to communicate state of async processes.
// the queue manager will launch a channel monitor to recieve messages and update the state of that async operation.
// will return an error if a channel has already been launched. Channels must be closed by the recipient after use.
// job state will be persisted and retained for a period of time before being cleaned up.
func (q *AsyncJobManager) RequestJobMessageQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error) {
	key := q.getJobMessageKey(brokerid, instanceid, operation)
	if _, present := q.brokerQueues[*key]; !present {
		jobchan := make(chan AsyncJobMsg)
		q.brokerQueues[*key] = jobchan
		go q.msgProcessor(jobchan, key)
		return jobchan, nil
	}
	return nil, fmt.Errorf("taskqueue: a job queue already exists for that key: %v", key)
}

// a broker or adapter can query the state of a job, will return an error if there is no known state.
// jobstates get cleaned-up automatically after a period of time after the chan is closed
// we cant do clean up here because state means different things to different brokers
func (q *AsyncJobManager) GetJobState(brokerid string, instanceid string, operation base.Operation) (*AsyncJobState, error) {
	key := &AsyncJobKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
	if state, present := q.jobStates[*key]; present {
		return &state, nil
	}
	return &AsyncJobState{}, fmt.Errorf("taskqueue: no state found for that key: %v", key)
}
