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
	DataDir, _ := ioutil.TempDir("", "storetest")
	defer os.RemoveAll(DataDir)

	bind0 := "127.0.0.1:4000"
	bind1 := "127.0.0.1:4001"
	bind2 := "127.0.0.1:4002"

	b0 := &cluster.Broker{
		Host: "127.0.0.1",
		Port: "4000",
		ID:   0,
	}
	b1 := &cluster.Broker{
		Host: "127.0.0.1",
		Port: "4001",
		ID:   1,
	}
	b2 := &cluster.Broker{
		Host: "127.0.0.1",
		Port: "4002",
		ID:   2,
	}

	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "jocko/brokertest")
	s0 := New(Options{
		DataDir:              filepath.Join(DataDir, "0"),
		RaftAddr:             bind0,
		Logger:               logger,
		TCPAddr:              bind0,
		ID:                   0,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b1, b2},
	})
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	s1 := New(Options{
		DataDir:              filepath.Join(DataDir, "1"),
		RaftAddr:             bind1,
		Logger:               logger,
		TCPAddr:              bind1,
		ID:                   1,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b0, b2},
	})
	err = s1.Open()
	assert.NoError(t, err)
	defer s1.Close()

	s2 := New(Options{
		DataDir:              filepath.Join(DataDir, "2"),
		RaftAddr:             bind2,
		TCPAddr:              bind2,
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
