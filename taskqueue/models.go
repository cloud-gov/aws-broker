package taskqueue

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

// messages of job state delivered over chan that are persisted
type AsyncJobMsg struct {
	BrokerId        string         `gorm:"primary_key; not null"`
	InstanceId      string         `gorm:"primary_key; not null"`
	JobType         base.Operation `gorm:"primary_key; not null"`
	JobState        AsyncJobState  `gorm:"embedded"`
	ProcessedStatus chan bool      `gorm:"-"`
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
