package jobs

import (
	"time"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/go-co-op/gocron"
)

// job state object persisted for brokers to access
type AsyncJobState struct {
	State   base.InstanceState
	Message string
}

// messages of asynchronous job state. This struct is used as a model for
// storing job states which are persisted to the database.
type AsyncJobMsg struct {
	BrokerId        string         `gorm:"primaryKey; not null"`
	InstanceId      string         `gorm:"primaryKey; not null"`
	JobType         base.Operation `gorm:"primaryKey; not null"`
	JobState        AsyncJobState  `gorm:"embedded"`
	ProcessedStatus chan bool      `gorm:"-"`
}

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
	jobStates    map[AsyncJobKey]AsyncJobState
	brokerQueues map[AsyncJobKey]chan AsyncJobMsg
	cleanup      map[AsyncJobKey]time.Time
	scheduler    *gocron.Scheduler
	expiration   time.Duration
	check        time.Duration
}
