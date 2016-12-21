package broker

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
)

func TestFetchMessages(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "storetest")
	defer os.RemoveAll(dataDir)

	raft0 := "127.0.0.1:4000"
	raft1 := "127.0.0.1:4001"
	raft2 := "127.0.0.1:4002"

	b0 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "3000",
		RaftAddr: raft0,
		ID:       0,
	}
	b1 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "4001",
		RaftAddr: raft1,
		ID:       1,
	}
	b2 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "4002",
		RaftAddr: raft2,
		ID:       2,
	}

	logger := simplelog.New(os.Stdout, simplelog.INFO, "jocko/replicator_test")
	s0 := New(0,
		OptionDataDir(filepath.Join(dataDir, "0")),
		OptionLogDir(filepath.Join(dataDir, "0")),
		OptionRaftAddr(raft0),
		OptionTCPAddr("127.0.0.1:3000"),
		OptionBrokers([]*jocko.BrokerConn{b1, b2}),
		OptionLogger(logger),
	)
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	s1 := New(1,
		OptionDataDir(filepath.Join(dataDir, "1")),
		OptionLogDir(filepath.Join(dataDir, "1")),
		OptionRaftAddr(raft1),
		OptionTCPAddr(raft1),
		OptionBrokers([]*jocko.BrokerConn{b0, b2}),
		OptionLogger(logger),
	)
	err = s1.Open()
	assert.NoError(t, err)
	defer s1.Close()

	s2 := New(2,
		OptionDataDir(filepath.Join(dataDir, "2")),
		OptionLogDir(filepath.Join(dataDir, "2")),
		OptionRaftAddr(raft2),
		OptionTCPAddr(raft2),
		OptionBrokers([]*jocko.BrokerConn{b0, b1}),
		OptionLogger(logger),
	)
	err = s2.Open()
	assert.NoError(t, err)
	defer s2.Close()

	l, err := s0.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	tp := &jocko.Partition{
		Topic:           "test",
		ID:              0,
		Leader:          b0,
		PreferredLeader: b0,
	}

	var peer, leader *Broker
	bs := []*Broker{s0, s1, s2}
	for _, b := range bs {
		if b.raftAddr == l {
			leader = b
		} else {
			peer = b
		}
	}

	err = leader.AddPartition(tp)
	assert.NoError(t, err)

	err = s0.WaitForAppliedIndex(2, 10*time.Second)
	assert.NoError(t, err)

	isLeader := s0.IsLeaderOfPartition(tp.Topic, tp.ID, tp.LeaderID())
	assert.True(t, isLeader)

	err = peer.WaitForAppliedIndex(2, 10*time.Second)
	assert.NoError(t, err)

	server := server.New(s0.tcpAddr, s0, logger)
	err = server.Start()
	assert.NoError(t, err)
	defer server.Close()

	replicator, err := NewPartitionReplicator(tp, 0,
		ReplicatorOptionMinBytes(5),
		ReplicatorOptionMaxWaitTime(int32(time.Millisecond*250)))
	assert.NoError(t, err)
	defer replicator.Close()

	msgs := []*protocol.Message{
		{Value: []byte("msg 0")},
		{Value: []byte("msg 1")},
	}
	mss := []*protocol.MessageSet{{
		Offset:   0,
		Messages: msgs,
	}, {
		Offset:   1,
		Messages: msgs,
	}}

	p, err := s0.Partition("test", 0)
	assert.NoError(t, err)

	for _, ms := range mss {
		encMs, err := protocol.Encode(ms)
		assert.NoError(t, err)

		offset, err := p.Append(encMs)
		assert.NoError(t, err)
		assert.Equal(t, ms.Offset, offset)
	}

	go replicator.fetchMessages()

	var i int
	for ms := range replicator.msgs {
		decMs := new(protocol.MessageSet)
		err = protocol.Decode([]byte(ms), decMs)
		assert.NoError(t, err)
		assert.Equal(t, int64(i), decMs.Offset)
		assert.Equal(t, 0, bytes.Compare(decMs.Messages[0].Value, decMs.Messages[0].Value))
		i++
		if i == len(mss) {
			break
		}
	}
}
