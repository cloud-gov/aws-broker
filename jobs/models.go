package jobs

import (
	"time"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/go-co-op/gocron"
)

// Jobs are unique for a broker,instance, and operation (CreateOp,DeleteOp,ModifyOp, BindOp, UnBindOp)
// this identifier is used as the unique key to retrieve a chan and or job state
type AsyncJobKey struct {
	BrokerId   string
	InstanceId string
	Operation  base.Operation
}

// AsyncJobManager maintains:
//
//	 	A set of open channels for active jobs
//		A list of jobstates for requested job
//		A task scheduler for cleanup of jobstates
//		A list of jobstates that need cleanup
type AsyncJobManager struct {
	jobStates    map[AsyncJobKey]asyncmessage.AsyncJobState
	brokerQueues map[AsyncJobKey]chan asyncmessage.AsyncJobMsg
	cleanup      map[AsyncJobKey]time.Time
	scheduler    *gocron.Scheduler
	expiration   time.Duration
	check        time.Duration
}
