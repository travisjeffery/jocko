package broker

import (
	"io"

	"github.com/hashicorp/raft"
)

func (s *Broker) Restore(rc io.ReadCloser) error {
	return nil
}

type FSMSnapshot struct {
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *FSMSnapshot) Release() {}

func (s *Broker) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}
