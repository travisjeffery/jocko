package cluster

import "github.com/travisjeffery/jocko/commitlog"

type PartitionOptions struct {
	Topic string
}

type Partition struct {
	PartitionOptions
	ID        int
	CommitLog *commitlog.CommitLog
}

func NewPartition(opts PartitionOptions) *Partition {
	return &Partition{
		Options: opts,
	}
}
