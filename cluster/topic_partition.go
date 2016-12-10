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
	Replicas        []*Broker `json:"replicas"`
	Leader          *Broker   `json:"leader"`
	PreferredLeader *Broker   `json:"preferred_leader"`

	CommitLog *commitlog.CommitLog `json:"-"`
}

// Strings returns the topic/partition as a string.
func (p TopicPartition) String() string {
	return fmt.Sprintf("%s/%d", p.Topic, p.Partition)
}

// OpenCommitLog opens a commit log for the partition at the path.
func (partition *TopicPartition) OpenCommitLog(logDir string) error {
	var err error
	partition.CommitLog, err = commitlog.New(commitlog.Options{
		Path:            path.Join(logDir, partition.String()),
		MaxSegmentBytes: 1024,
		MaxLogBytes:     -1,
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
