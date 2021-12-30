package taskqueue

import (
	"fmt"

	"github.com/18F/aws-broker/base"
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
//	 	A set of channels for active jobs
// 		A list of job states

type QueueManager struct {
	JobStates    map[AsyncJobQueueKey]AsyncJobState
	brokerQueues map[AsyncJobQueueKey]chan AsyncJobMsg
}

// must be called to initialize the manager
func Init() *QueueManager {
	mgr := &QueueManager{
		JobStates:    make(map[AsyncJobQueueKey]AsyncJobState),
		brokerQueues: make(map[AsyncJobQueueKey]chan AsyncJobMsg),
	}
	return mgr
}

// update the state list for the unique job
func (q *QueueManager) process(msg AsyncJobMsg) {
	key := &AsyncJobQueueKey{
		BrokerId:   msg.BrokerId,
		InstanceId: msg.InstanceId,
		Operation:  msg.JobType,
	}
	q.JobStates[*key] = msg.JobState
}

// async job monitor will process any messages
// coming in on the channel and then update the state for that job
func (q *QueueManager) MsgProcessor(jobChan chan AsyncJobMsg, key *AsyncJobQueueKey) {

	for job := range jobChan {
		q.process(job)
	}
	delete(q.brokerQueues, *key)
}

// a broker or adapter can request a channel to communicate state of async processes.
// the queue manager will launch a channel monitor to recieve messages and update the state of that async operation.
// will return an error if a channel has already been launched.
func (q *QueueManager) RequestQueue(brokerid string, instanceid string, operation base.Operation) (chan AsyncJobMsg, error) {
	key := &AsyncJobQueueKey{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		Operation:  operation,
	}
	if _, present := q.brokerQueues[*key]; !present {
		jobchan := make(chan AsyncJobMsg)
		q.brokerQueues[*key] = jobchan
		go q.MsgProcessor(jobchan, key)
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
	if state, present := q.JobStates[*key]; present {
		return &state, nil
	}
	return &AsyncJobState{}, fmt.Errorf("taskque: no state found for that key: %v", key)
}
