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
	if len(quemgr.jobStates) != 1 {
		t.Error("Jobstates failed to initialize")
	}
	time.Sleep(100 * time.Millisecond)
	quemgr.cleanupJobStates()
	if len(quemgr.jobStates) > 0 {
		t.Error("Jobstates failed to cleanup")
	}
}

func TestScheduleTask(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.ScheduleTask("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	quemgr.scheduler.Stop()
}

func TestUnScheduleTask(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.ScheduleTask("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	quemgr.UnScheduleTask("test")
	if quemgr.scheduler.Len() != 0 {
		t.Error("Jobs are not = 0")
	}
	quemgr.scheduler.Stop()
}

func TestIsTaskScheduled(t *testing.T) {
	quemgr := NewQueueManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.ScheduleTask("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	if !quemgr.IsTaskScheduled("test") {
		t.Error("could not fine test job")
	}
	quemgr.scheduler.Stop()

}
