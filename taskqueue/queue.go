package taskqueue

import (
	"fmt"
	"time"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/go-co-op/gocron"
)

type QueueManager interface {
	ScheduleTask(cronExpression string, id string, task interface{}) (*gocron.Job, error)
	UnScheduleTask(id string) error
	IsTaskScheduled(id string) bool
	RequestTaskQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error)
	GetTaskState(brokerid string, instanceid string, operation base.Operation) (*AsyncJobState, error)
	TaskQueueExists(brokerid string, instanceid string, operation base.Operation) bool
}

// job state object persisted for brokers to access
type AsyncJobState struct {
	State   base.InstanceState
	Message string
}

// messages of job state delivered over chan that are persisted
type AsyncJobMsg struct {
	BrokerId        string         `gorm:"primary_key; not null"`
	InstanceId      string         `gorm:"primary_key; not null"`
	JobType         base.Operation `gorm:"primary_key; not null"`
	JobState        AsyncJobState  `gorm:"embedded"`
	ProcessedStatus chan bool      `sql:"-"`
}

// Jobs are unique for a broker,instance, and operation (CreateOp,DeleteOp,ModifyOp, BindOp, UnBindOp)
// this identifier is used as the unique key to retrieve a chan and or job state
type AsyncJobQueueKey struct {
	BrokerId   string
	InstanceId string
	Operation  base.Operation
}

// TaskQueueManager maintains:
//
//	 	A set of open channels for active jobs
//		A list of jobstates for requested job
//		A task scheduler for cleanup of jobstates
//		A list of jobstates that need cleanup
type TaskQueueManager struct {
	jobStates    map[AsyncJobQueueKey]AsyncJobState
	brokerQueues map[AsyncJobQueueKey]chan AsyncJobMsg
	cleanup      map[AsyncJobQueueKey]time.Time
	scheduler    *gocron.Scheduler
	expiration   time.Duration
	check        time.Duration
}

// can be called to initialize the manager
// defaults to do clean-up of jobstates after an hour.
// runs clean up check every 15 minutes
func NewTaskQueueManager() *TaskQueueManager {
	mgr := &TaskQueueManager{
		jobStates:    make(map[AsyncJobQueueKey]AsyncJobState),
		brokerQueues: make(map[AsyncJobQueueKey]chan AsyncJobMsg),
		cleanup:      make(map[AsyncJobQueueKey]time.Time),
		scheduler:    gocron.NewScheduler(time.Local),
		expiration:   5 * time.Minute, //platform issues last-operation calls every 2 minutes
		check:        2 * time.Minute,
	}
	return mgr
}

// must be called to activate cleanup mechanism
// separated from constructor to allow config and testing
func (q *TaskQueueManager) Init() {
	q.scheduler.TagsUnique()
	q.scheduler.Every(q.check).Tag("QueueCleaner").Do(q.cleanupJobStates)
	q.scheduler.StartAsync()
}

// Allow Jobs to be scheduled by brokers
func (q *TaskQueueManager) ScheduleTask(cronExpression string, id string, task interface{}) (*gocron.Job, error) {
	return q.scheduler.Cron(cronExpression).Tag(id).Do(task)
}

// Stop jobs scheduled
func (q *TaskQueueManager) UnScheduleTask(id string) error {
	return q.scheduler.RemoveByTag(id)
}

// Determine if job(id) is scheduled
func (q *TaskQueueManager) IsTaskScheduled(id string) bool {
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
func (q *TaskQueueManager) processMsg(msg AsyncJobMsg) {
	key := &AsyncJobQueueKey{
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
func (q *TaskQueueManager) msgProcessor(jobChan chan AsyncJobMsg, key *AsyncJobQueueKey) {
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
func (q *TaskQueueManager) cleanupJobStates() {
	now := time.Now()
	for key, due := range q.cleanup {
		if now.After(due) {
			delete(q.jobStates, key)
			delete(q.cleanup, key)
		}
	}
}

func (q *TaskQueueManager) getTaskQueueJobKey(brokerid string, instanceid string, operation base.Operation) *AsyncJobQueueKey {
	return &AsyncJobQueueKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
}

// a broker or adapter can request a channel to communicate state of async processes.
// the queue manager will launch a channel monitor to recieve messages and update the state of that async operation.
// will return an error if a channel has already been launched. Channels must be closed by the recipient after use.
// job state will be persisted and retained for a period of time before being cleaned up.
func (q *TaskQueueManager) RequestTaskQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error) {
	key := q.getTaskQueueJobKey(brokerid, instanceid, operation)
	if _, present := q.brokerQueues[*key]; !present {
		jobchan := make(chan AsyncJobMsg)
		q.brokerQueues[*key] = jobchan
		go q.msgProcessor(jobchan, key)
		return jobchan, nil
	}
	return nil, fmt.Errorf("taskqueue: a job queue already exists for that key: %v", key)
}

func (q *TaskQueueManager) TaskQueueExists(brokerid string, instanceid string, operation base.Operation) bool {
	key := q.getTaskQueueJobKey(brokerid, instanceid, operation)
	_, present := q.brokerQueues[*key]
	return present
}

// a broker or adapter can query the state of a job, will return an error if there is no known state.
// jobstates get cleaned-up automatically after a period of time after the chan is closed
// we cant do clean up here because state means different things to different brokers
func (q *TaskQueueManager) GetTaskState(brokerid string, instanceid string, operation base.Operation) (*AsyncJobState, error) {
	key := &AsyncJobQueueKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
	if state, present := q.jobStates[*key]; present {
		return &state, nil
	}
	return &AsyncJobState{}, fmt.Errorf("taskqueue: no state found for that key: %v", key)
}
