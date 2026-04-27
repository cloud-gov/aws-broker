package asyncmessage

import "github.com/cloud-gov/aws-broker/base"

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
