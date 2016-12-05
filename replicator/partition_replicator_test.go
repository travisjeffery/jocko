package replicator

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/cluster"
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

	b0 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "3000",
		RaftAddr: raft0,
		ID:       0,
	}
	b1 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "4001",
		RaftAddr: raft1,
		ID:       1,
	}
	b2 := &cluster.Broker{
		Host:     "127.0.0.1",
		Port:     "4002",
		RaftAddr: raft2,
		ID:       2,
	}

	logger := simplelog.New(os.Stdout, simplelog.INFO, "jocko/replicator_test")
	s0 := broker.New(broker.Options{
		DataDir:              filepath.Join(dataDir, "0"),
		LogDir:               filepath.Join(dataDir, "0"),
		RaftAddr:             raft0,
		Logger:               logger,
		TCPAddr:              "127.0.0.1:3000",
		ID:                   0,
		DefaultNumPartitions: 2,
		Brokers:              []*cluster.Broker{b1, b2},
	})
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	s1 := broker.New(broker.Options{
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

	s2 := broker.New(broker.Options{
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

	var peer, leader *broker.Broker
	bs := []*broker.Broker{s0, s1, s2}
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

	server := server.New(s0.TCPAddr, s0, logger)
	err = server.Start()
	assert.NoError(t, err)
	defer server.Close()

	replicator, err := NewPartitionReplicator(&Options{
		MinBytes:    5,
		MaxWaitTime: int32(time.Millisecond * 250),
		Partition:   &tp,
	})
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

		offset, err := p.CommitLog.Append(encMs)
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
