package broker

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/jocko"
)

func newCommand(cmd jocko.RaftCmdType, data interface{}) (c jocko.RaftCommand, err error) {
	var b []byte
	b, err = json.Marshal(data)
	if err != nil {
		return c, err
	}
	r := json.RawMessage(b)
	return jocko.RaftCommand{
		Cmd:  cmd,
		Data: &r,
	}, nil
}

// setupRaft is used to configure and create the raft node
func (b *Broker) setupRaft() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", b.raftAddr)
	if err != nil {
		return err
	}

	if b.raftTransport == nil {
		b.raftTransport, err = raft.NewTCPTransport(addr.String(), nil, 3, timeout, os.Stderr)
		if err != nil {
			return errors.Wrap(err, "tcp transport failed")
		}
	}

	path := filepath.Join(b.dataDir, raftState)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	b.raftPeers = raft.NewJSONPeers(path, b.raftTransport)

	var peers []string
	for _, p := range b.peers {
		addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.RaftPort}
		peers = append(peers, addr.String())
	}
	if err = b.raftPeers.SetPeers(peers); err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(path, 2, os.Stderr)
	if err != nil {
		return err
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	b.raftStore = boltStore

	leaderCh := make(chan bool, 1)
	b.raftLeaderCh = leaderCh
	b.raftConfig.NotifyCh = leaderCh
	b.raftConfig.StartAsLeader = !b.devDisableBootstrap

	raft, err := raft.NewRaft(b.raftConfig, b, boltStore, boltStore, snapshots, b.raftPeers, b.raftTransport)
	if err != nil {
		if b.raftStore != nil {
			b.raftStore.Close()
		}
		b.raftTransport.Close()
		return errors.Wrap(err, "raft failed")
	}
	b.raft = raft

	return nil
}

func (s *Broker) raftApply(cmdType jocko.RaftCmdType, data interface{}) error {
	c, err := newCommand(cmdType, data)
	if err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, timeout)
	return f.Error()
}
