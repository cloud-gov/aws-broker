package taskqueue

// import (
// 	"github.com/cloud-gov/aws-broker/base"
// )

// type TaskState uint8

// const (
// 	TaskRunning TaskState = iota // 0
// 	TaskFailed
// 	TaskCompleted
// )

// func (t TaskState) String() string {
// 	switch t {
// 	case TaskRunning:
// 		return "running"
// 	case TaskFailed:
// 		return "failed"
// 	case TaskCompleted:
// 		return "completed"
// 	default:
// 		return "unknown"
// 	}
// }

// type AsyncTask struct {
// 	BrokerId   string         `gorm:"primary_key; not null"`
// 	InstanceId string         `gorm:"primary_key; not null"`
// 	Operation  base.Operation `gorm:"primary_key; not null"`
// 	State      TaskState
// 	Message    string
// }
