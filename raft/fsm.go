package raft

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko"
)

type fsm struct {
	logger    log.Logger
	commandCh chan<- jocko.RaftCommand
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

// Apply forwards the commands received over command channel to be
// applied by broker
func (s *fsm) Apply(l *raft.Log) interface{} {
	var c jocko.RaftCommand
	if err := json.Unmarshal(l.Data, &c); err != nil {
		s.logger.Info("json unmarshal failed: bad raft command")
		return nil
	}
	s.commandCh <- c
	return nil
}
