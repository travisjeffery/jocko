package broker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/simplelog"
)

func TestStoreOpen(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "broker_test")
	defer os.RemoveAll(dataDir)

	raft0 := "127.0.0.1:5000"
	raft1 := "127.0.0.1:5001"
	raft2 := "127.0.0.1:5002"

	b0 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "3001",
		RaftAddr: raft0,
		ID:       0,
	}
	b1 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "5001",
		RaftAddr: raft1,
		ID:       1,
	}
	b2 := &jocko.BrokerConn{
		Host:     "127.0.0.1",
		Port:     "5002",
		RaftAddr: raft2,
		ID:       2,
	}

	logger := simplelog.New(os.Stdout, simplelog.INFO, "jocko/broker_test")
	s0 := New(
		0,
		filepath.Join(dataDir, "0"),
		filepath.Join(dataDir, "0"),
		raft0,
		"127.0.0.1:3001",
		[]*jocko.BrokerConn{b1, b2},
		logger)
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	s1 := New(
		1,
		filepath.Join(dataDir, "1"),
		filepath.Join(dataDir, "1"),
		raft1,
		raft1,
		[]*jocko.BrokerConn{b0, b2},
		logger,
	)
	err = s1.Open()
	assert.NoError(t, err)
	defer s1.Close()

	s2 := New(
		2,
		filepath.Join(dataDir, "2"),
		filepath.Join(dataDir, "2"),
		raft2,
		raft2,
		[]*jocko.BrokerConn{b0, b1},
		logger,
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

	// check that consensus was made to peer
	ps, err := peer.TopicPartitions(tp.Topic)
	assert.NoError(t, err)
	for _, p := range ps {
		assert.Equal(t, tp.Topic, p.Topic)
		assert.Equal(t, tp.LeaderID(), p.LeaderID())
	}
}
