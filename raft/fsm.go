package raft

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
)

type fsm struct {
	//commandCh chan<- jocko.RaftCommand
	broker jocko.Broker
}

func (s *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

type FSMSnapshot struct {
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *FSMSnapshot) Release() {}

func (s *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}

// Apply raft command as fsm
func (s *fsm) Apply(l *raft.Log) interface{} {
	var c jocko.RaftCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(errors.Wrap(err, "json unmarshal failed"))
	}
	//s.commandCh <- c
	s.broker.Apply(c)
	return nil
}
