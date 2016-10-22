package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStoreOpen(t *testing.T) {
	DataDir, _ := ioutil.TempDir("", "storetest")
	defer os.RemoveAll(DataDir)
	BindAddr := "127.0.0.1:4000"

	s0 := New(Options{
		DataDir:       DataDir,
		BindAddr:      BindAddr,
		numPartitions: 2,
	})
	assert.NotNil(t, s0)

	err := s0.Open()
	assert.NoError(t, err)
	defer s0.Close()

	_, err = s0.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	DataDir, _ = ioutil.TempDir("", "storetest")
	defer os.RemoveAll(DataDir)
	BindAddr = "127.0.0.1:4001"
	s1 := New(Options{
		DataDir:       DataDir,
		BindAddr:      BindAddr,
		numPartitions: 2,
	})
	err = s1.Open()
	assert.NoError(t, err)
	defer s1.Close()

	err = s0.Join(s1.BrokerID())
	assert.NoError(t, err)

	tp := TopicPartition{
		Topic:           "test",
		Partition:       0,
		Leader:          s0.BrokerID(),
		PreferredLeader: s0.BrokerID(),
		Replicas:        []string{s0.BrokerID()},
	}

	err = s0.AddPartition(tp)
	assert.NoError(t, err)

	isLeader := s0.IsLeaderOfPartition(tp)
	assert.True(t, isLeader)

	err = s1.WaitForAppliedIndex(2, 10*time.Second)
	assert.NoError(t, err)

	ps, err := s1.Partitions()
	assert.NoError(t, err)
	for _, p := range ps {
		assert.Equal(t, tp.Topic, p.Topic)
		assert.Equal(t, tp.Leader, p.Leader)
	}
}
