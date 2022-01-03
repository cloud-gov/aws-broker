package taskqueue

import (
	"testing"
	"time"

	"github.com/18F/aws-broker/base"
)

var brokerid string = "mybroker"
var instanceid string = "myinstance"
var jobop base.Operation = base.DeleteOp
var jobstate base.InstanceState = base.InstanceInProgress
var jobmsg string = "testing in-progress"

var testAsyncJobKey AsyncJobQueueKey = AsyncJobQueueKey{
	BrokerId:   brokerid,
	InstanceId: instanceid,
	Operation:  jobop,
}

func TestRequestJobQueue(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestQueue(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestQueue failed! %v", err)
	}
	jobchan <- AsyncJobMsg{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		JobType:    jobop,
		JobState: AsyncJobState{
			State:   jobstate,
			Message: jobmsg,
		},
	}
	if len(quemgr.brokerQueues) != 1 {
		t.Error("BrokerQueue has more than one channel registered!")
	}
	close(jobchan)
	time.Sleep(100 * time.Millisecond)
	if len(quemgr.brokerQueues) != 0 {
		t.Error("BrokerQueue has more than zero channels registered!")
	}
}

func TestGetJobState(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestQueue(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestQueue failed! %v", err)
	}
	jobchan <- AsyncJobMsg{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		JobType:    jobop,
		JobState: AsyncJobState{
			State:   jobstate,
			Message: jobmsg,
		},
	}
	if len(quemgr.brokerQueues) != 1 {
		t.Error("BrokerQueue has more than one channel registered!")
	}
	close(jobchan)
	state, err := quemgr.GetJobState(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestJobState failed! %v", err)
	}
	if state.State != jobstate {
		t.Errorf("Jobstate is unexpected: %s", jobstate.String())
	}
}

func TestCleanUpJobState(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestQueue(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestQueue failed! %v", err)
	}
	jobchan <- AsyncJobMsg{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		JobType:    jobop,
		JobState: AsyncJobState{
			State:   jobstate,
			Message: jobmsg,
		},
	}
	if len(quemgr.brokerQueues) != 1 {
		t.Error("BrokerQueue has more than one channel registered!")
	}
	close(jobchan)
	time.Sleep(100 * time.Millisecond)
	quemgr.cleanupJobStates()
	if len(quemgr.jobStates) > 0 {
		t.Error("Jobstates failed to cleanup")
	}
}
