package jocko

import "github.com/travisjeffery/jocko/commitlog"

type Options struct {
	Topic string
}

type Partition struct {
	Options
	ID        int
	CommitLog *commitlog.CommitLog
}

func NewPartition(opts Options) *Partition {
	return &Partition{
		Options: opts,
	}
}
