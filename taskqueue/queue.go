package taskqueue

import (
	"fmt"
	"time"

	"github.com/18F/aws-broker/base"
	"github.com/go-co-op/gocron"
)

type AsyncJobState struct {
	State   base.InstanceState
	Message string
}

type AsyncJobMsg struct {
	BrokerId   string
	InstanceId string
	JobType    base.Operation
	JobState   AsyncJobState
}

// operations are unique for a broker,instance, and operation (CreateOp,DeleteOp,ModifyOp, BindOp, UnBindOp)
type AsyncJobQueueKey struct {
	BrokerId   string
	InstanceId string
	Operation  base.Operation
}

// QueueManager maintains:
//	 	A set of open channels for active jobs
// 		A list of jobstates for requested job
//		A task scheduler for cleanup of jobstates
// 		A list of jobstates that need cleanup
type QueueManager struct {
	jobStates    map[AsyncJobQueueKey]AsyncJobState
	brokerQueues map[AsyncJobQueueKey]chan AsyncJobMsg
	cleanup      map[AsyncJobQueueKey]time.Time
	scheduler    *gocron.Scheduler
	expiration   time.Duration
	check        time.Duration
}

// can be called to initialize the manager
func NewQueueManager() *QueueManager {
	mgr := &QueueManager{
		jobStates:    make(map[AsyncJobQueueKey]AsyncJobState),
		brokerQueues: make(map[AsyncJobQueueKey]chan AsyncJobMsg),
		cleanup:      make(map[AsyncJobQueueKey]time.Time),
		scheduler:    gocron.NewScheduler(time.Local),
		expiration:   time.Hour,
		check:        15 * time.Minute,
	}
	return mgr
}

// must be called to activate clean mechanism
// separated from constructor to allow config and testing
func (q *QueueManager) Init() {
	q.scheduler.Every(q.check).Do(q.cleanupJobStates)
	q.scheduler.StartAsync()
}

// update the state list for the unique job
func (q *QueueManager) processMsg(msg AsyncJobMsg) {
	key := &AsyncJobQueueKey{
		BrokerId:   msg.BrokerId,
		InstanceId: msg.InstanceId,
		Operation:  msg.JobType,
	}
	q.jobStates[*key] = msg.JobState
}

// async job monitor will process any messages
// coming in on the channel and then update the state for that job
func (q *QueueManager) msgProcessor(jobChan chan AsyncJobMsg, key *AsyncJobQueueKey) {

	for job := range jobChan {
		q.processMsg(job)
	}
	delete(q.brokerQueues, *key)
	q.cleanup[*key] = time.Now().Add(q.expiration)
}

// async cron job to remove expired jobstates
func (q *QueueManager) cleanupJobStates() {
	now := time.Now()
	for key, due := range q.cleanup {
		if now.After(due) {
			delete(q.jobStates, key)
			delete(q.cleanup, key)
		}
	}

}

// a broker or adapter can request a channel to communicate state of async processes.
// the queue manager will launch a channel monitor to recieve messages and update the state of that async operation.
// will return an error if a channel has already been launched. Channels must be closed by the recipient after use.
func (q *QueueManager) RequestQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error) {
	key := &AsyncJobQueueKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
	if _, present := q.brokerQueues[*key]; !present {
		jobchan := make(chan AsyncJobMsg)
		q.brokerQueues[*key] = jobchan
		go q.msgProcessor(jobchan, key)
		return jobchan, nil
	}
	return nil, fmt.Errorf("taskque: a job queue already exists for that key: %v ", key)
}

// a broker or adapter can query the state of a job, will return an error if there is no known state
func (q *QueueManager) GetJobState(brokerid string, instanceid string, operation base.Operation) (*AsyncJobState, error) {
	key := &AsyncJobQueueKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
	if state, present := q.jobStates[*key]; present {
		return &state, nil
	}
	return &AsyncJobState{}, fmt.Errorf("taskque: no state found for that key: %v", key)
}
