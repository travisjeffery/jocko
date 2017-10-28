package broker

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	nop jocko.RaftCmdType = iota
	createPartition
	deleteTopic
	// others
)

func (b *Broker) raftApply(cmd jocko.RaftCmdType, data interface{}) error {
	var bb []byte
	bb, err := json.Marshal(data)
	if err != nil {
		return err
	}
	r := json.RawMessage(bb)
	c := jocko.RaftCommand{
		Cmd:  cmd,
		Data: &r,
	}
	return b.raft.Apply(c)
}

// handleRaftCommands reads commands sent into the given channel to apply them.
func (b *Broker) handleRaftCommmands(commandCh <-chan jocko.RaftCommand) {
	for {
		select {
		case cmd := <-commandCh:
			b.apply(cmd)
		case <-b.shutdownCh:
			return
		}
	}
}

// apply applies the given command on this broker.
func (b *Broker) apply(c jocko.RaftCommand) {
	defer func() {
		if r := recover(); r != nil {
			b.logger.Info("error while applying raft command: %v", r)
			b.Shutdown()
		}
	}()

	b.logger.Debug("broker/apply cmd %d:\n%s", c.Cmd, c.Data)
	panic(c)
	switch c.Cmd {
	case nop:
		return
	case createPartition:
		p := new(jocko.Partition)
		if err := unmarshalData(c.Data, p); err != nil {
			b.logger.Info("received malformed raft command: %v", err)
			// TODO: should panic?
			return
		}
		if err := b.startReplica(p); err != protocol.ErrNone {
			panic(err)
		}
	case deleteTopic:
		p := new(jocko.Partition)
		if err := unmarshalData(c.Data, p); err != nil {
			b.logger.Info("received malformed raft command: %v", err)
			// TODO: should panic?
			return
		}
		if err := b.deletePartitions(p); err != nil {
			panic(errors.Wrap(err, "topic delete failed"))
		}
	}
}
