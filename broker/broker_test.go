package broker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/cluster"
	"github.com/travisjeffery/simplelog"
)

func TestStoreOpen(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "broker_test")
	defer os.RemoveAll(dataDir)

	raft0 := "127.0.0.1:5000"
	raft1 := "127.0.0.1:5001"
	raft2 := "127.0.0.1:5002"

	b0 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "3001",
		RaftAddr: raft0,
		ID:       0,
	}
	b1 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "5001",
		RaftAddr: raft1,
		ID:       1,
	}
	b2 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "5002",
		RaftAddr: raft2,
		ID:       2,
	}

	logger := simplelog.New(os.Stdout, simplelog.INFO, "jocko/broker_test")
	s0 := New(Options{
		DataDir:              filepath.Join(dataDir, "0"),
		LogDir:               filepath.Join(dataDir, "0"),
		RaftAddr:             raft0,
		Logger:               logger,
		TCPAddr:              "127.0.0.1:3001",
		ID:                   0,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b1, b2},
	})
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	s1 := New(Options{
		DataDir:              filepath.Join(dataDir, "1"),
		LogDir:               filepath.Join(dataDir, "1"),
		RaftAddr:             raft1,
		Logger:               logger,
		TCPAddr:              raft1,
		ID:                   1,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b0, b2},
	})
	err = s1.Open()
	assert.NoError(t, err)
	defer s1.Close()

	s2 := New(Options{
		DataDir:              filepath.Join(dataDir, "2"),
		LogDir:               filepath.Join(dataDir, "2"),
		RaftAddr:             raft2,
		TCPAddr:              raft2,
		Logger:               logger,
		ID:                   2,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b0, b1},
	})
	err = s2.Open()
	assert.NoError(t, err)
	defer s2.Close()

	l, err := s0.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	tp := cluster.TopicPartition{
		Topic:           "test",
		Partition:       0,
		Leader:          b0,
		PreferredLeader: b0,
		Replicas:        nil,
	}

	var peer, leader *Broker
	bs := []*Broker{s0, s1, s2}
	for _, b := range bs {
		if b.RaftAddr == l {
			leader = b
		} else {
			peer = b
		}
	}

	err = leader.AddPartition(tp)
	assert.NoError(t, err)

	err = s0.WaitForAppliedIndex(2, 10*time.Second)
	assert.NoError(t, err)

	isLeader := s0.IsLeaderOfPartition(&tp)
	assert.True(t, isLeader)

	err = peer.WaitForAppliedIndex(2, 10*time.Second)
	assert.NoError(t, err)

	// check that consensus was made to peer
	ps, err := peer.PartitionsForTopic(tp.Topic)
	assert.NoError(t, err)
	for _, p := range ps {
		assert.Equal(t, tp.Topic, p.Topic)
		assert.Equal(t, tp.Leader, p.Leader)
	}
}
