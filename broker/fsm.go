package broker

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
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

func (s *Broker) raftApply(cmd jocko.RaftCmdType, data interface{}) error {
	var b []byte
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	r := json.RawMessage(b)
	c := jocko.RaftCommand{
		Cmd:  cmd,
		Data: &r,
	}
	return s.raft.Apply(c)
}

// Apply raft command as fsm
func (s *Broker) Apply(c jocko.RaftCommand) {
	s.logger.Debug("broker/apply cmd [%d]", c.Cmd)
	switch c.Cmd {
	case addPartition:
		p := new(jocko.Partition)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json marshal failed"))
		}
		if err := json.Unmarshal(b, p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := s.StartReplica(p); err != nil {
			panic(errors.Wrap(err, "start replica failed"))
		}
	case deleteTopic:
		p := new(jocko.Partition)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json marshal failed"))
		}
		if err := json.Unmarshal(b, p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := s.deleteTopic(p); err != nil {
			panic(errors.Wrap(err, "topic delete failed"))
		}
	}
}
