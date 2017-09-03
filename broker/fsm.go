package broker

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
)

const (
	addPartition jocko.RaftCmdType = iota
	deleteTopic
	// others
)

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

// handleRaftCommands reads commands sent into the given channel to apply them.
func (s *Broker) handleRaftCommmands(commandCh <-chan jocko.RaftCommand) {
	for {
		select {
		case cmd := <-commandCh:
			s.apply(cmd)
		case <-s.shutdownCh:
			return
		}
	}
}

// apply applies the given command on this broker.
func (s *Broker) apply(c jocko.RaftCommand) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Info("error while applying raft command: %v", r)
			s.Shutdown()
		}
	}()

	s.logger.Debug("broker/apply cmd %d:\n%s", c.Cmd, c.Data)
	switch c.Cmd {
	case addPartition:
		p := new(jocko.Partition)
		if err := unmarshalData(c.Data, p); err != nil {
			s.logger.Info("received malformed raft command: %v", err)
			return
		}
		if err := s.StartReplica(p); err != nil {
			panic(errors.Wrap(err, "start replica failed"))
		}
	case deleteTopic:
		p := new(jocko.Partition)
		if err := unmarshalData(c.Data, p); err != nil {
			s.logger.Info("received malformed raft command: %v", err)
			return
		}
		if err := s.deleteTopic(p); err != nil {
			panic(errors.Wrap(err, "topic delete failed"))
		}
	}
}
