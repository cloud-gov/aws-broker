package rds

import (
	"context"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/riverqueue/river"
)

const (
	CreateKind = "rds-create"
)

type CreateArgs struct {
	i        *RDSInstance
	plan     *catalog.RDSPlan
	password string
}

func (CreateArgs) Kind() string { return CreateKind }

type CreateWorker struct {
	river.WorkerDefaults[CreateArgs]
	dbAdapter dedicatedDBAdapter
}

func (w *CreateWorker) Work(ctx context.Context, job *river.Job[CreateArgs]) error {
	err := w.dbAdapter.asyncCreateDB(job.Args.i, job.Args.plan, job.Args.password)
	return err
}
