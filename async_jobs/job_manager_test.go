package async_jobs

import (
	"testing"
	"time"

	"github.com/cloud-gov/aws-broker/base"
)

var brokerid string = "mybroker"
var instanceid string = "myinstance"
var jobop base.Operation = base.DeleteOp
var jobstate base.InstanceState = base.InstanceInProgress
var jobmsg string = "testing in-progress"

func TestRequestJobMessageQueue(t *testing.T) {
	quemgr := NewAsyncJobManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestJobMessageQueue(brokerid, instanceid, jobop)
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
	quemgr := NewAsyncJobManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestJobMessageQueue(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestQueue failed! %v", err)
	}
	jobMsg := AsyncJobMsg{
		BrokerId:   brokerid,
		InstanceId: instanceid,
		JobType:    jobop,
		JobState: AsyncJobState{
			State:   jobstate,
			Message: jobmsg,
		},
		ProcessedStatus: make(chan bool),
	}
	jobchan <- jobMsg
	if len(quemgr.brokerQueues) != 1 {
		t.Error("BrokerQueue has more than one channel registered!")
	}

	close(jobchan)
	// receiving from the job message status ensures that it was received and processed
	jobMsgStatus := <-jobMsg.ProcessedStatus
	if jobMsgStatus != true {
		t.Fatalf("expected job message status: %t, got %t", true, jobMsgStatus)
	}

	state, err := quemgr.GetJobState(brokerid, instanceid, jobop)
	if err != nil {
		t.Errorf("RequestJobState failed! %v", err)
	}
	if state.State != jobstate {
		t.Errorf("Jobstate is unexpected: %s", jobstate.String())
	}
}

func TestCleanUpJobState(t *testing.T) {
	quemgr := NewAsyncJobManager()
	quemgr.expiration = 10 * time.Millisecond
	jobchan, err := quemgr.RequestJobMessageQueue(brokerid, instanceid, jobop)
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

func TestScheduleJob(t *testing.T) {
	quemgr := NewAsyncJobManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.scheduleJob("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	quemgr.scheduler.Stop()
}

func TestUnscheduleJob(t *testing.T) {
	quemgr := NewAsyncJobManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.scheduleJob("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	quemgr.unscheduleJob("test")
	if quemgr.scheduler.Len() != 0 {
		t.Error("Jobs are not = 0")
	}
	quemgr.scheduler.Stop()
}

func TestIsJobScheduled(t *testing.T) {
	quemgr := NewAsyncJobManager()
	quemgr.scheduler.StartAsync()
	_, err := quemgr.scheduleJob("*/1 * * * *", "test", quemgr.cleanupJobStates)
	if err != nil {
		t.Error("Test Task could not be schedule", err)
	}
	if quemgr.scheduler.Len() != 1 {
		t.Error("Jobs are not = 1")
	}
	if !quemgr.isJobScheduled("test") {
		t.Error("could not fine test job")
	}
	quemgr.scheduler.Stop()

}
