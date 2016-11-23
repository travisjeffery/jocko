package cluster

import (
	"fmt"
	"path"

	"github.com/travisjeffery/jocko/commitlog"
)

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`

	// broker ids
	Replicas        []int `json:"replicas"`
	Leader          int   `json:"leader"`
	PreferredLeader int   `json:"preferred_leader"`

	CommitLog *commitlog.CommitLog `json:"-"`
}

func (p TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", p.Topic, p.Partition)
}

// OpenCommitLog opens a commit log for the partition at the path.
func (partition *TopicPartition) OpenCommitLog(logDir string) error {
	var err error
	partition.CommitLog, err = commitlog.New(commitlog.Options{
		Path:            path.Join(logDir, partition.String()),
		MaxSegmentBytes: 1024,
	})
	if err != nil {
		return err
	}
	if err = partition.CommitLog.Init(); err != nil {
		return err
	}
	if err = partition.CommitLog.Open(); err != nil {
		return err
	}
	return nil
}
