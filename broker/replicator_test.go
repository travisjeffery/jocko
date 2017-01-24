package broker

import (
	"bytes"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

func TestBroker_Replicate(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "replicate_test")
	defer os.RemoveAll(dataDir)

	s0 := testServer(t, 0)
	defer s0.Shutdown()

	s0.WaitForLeader(10 * time.Second)

	addr := &net.TCPAddr{IP: net.ParseIP(s0.bindAddr), Port: s0.port}
	srv := server.New(addr.String(), s0, logger)
	err := srv.Start()
	assert.NoError(t, err)

	tp := &jocko.Partition{
		Topic:           "test",
		ID:              0,
		Leader:          0,
		PreferredLeader: 0,
		Replicas:        []int32{0},
	}

	err = s0.AddPartition(tp)
	assert.NoError(t, err)

	p, err := s0.Partition("test", 0)
	assert.NoError(t, err)


	t.Run("Replication Test", func(t *testing.T) {

		replicator := NewPartitionReplicator(p, 0,
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
	})

	t.Run("Delete Topic Test", func(t *testing.T) {
		init_topics := len(s0.Topics())
		s0.CreateTopic("topic1", 1)
		s0.CreateTopic("topic2", 1)
		assert.Equal(t, init_topics+2, len(s0.Topics()), "Topic count does not match." )
		assert.Contains(t, s0.Topics(), "topic1", "Topic does not present in list")
		s0.DeleteTopics("topic1")
		assert.NotContains(t, s0.Topics(), "topic1",
			"Topic 'topic1' still present in list, even after delete.")
		assert.Equal(t, init_topics+1, len(s0.Topics()), "Topic count does not match after delete.")
		s0.deleteTopic(p)
		assert.NotContains(t, s0.Topics(), "test",
			"Topic 'test' still present in list, even after delete.")

	})

	t.Run("Create Duplicate Topic Test", func(t *testing.T) {
		s0.CreateTopic("topic1", 1)
		error := s0.CreateTopic("topic1", 1)
		assert.Equal(t, error, ErrTopicExists, "Duplicate topic error does not match.")

	})

	t.Run("Test Broker shutdown", func(t *testing.T) {
		assert.Equal(t, s0.IsShutdown(), false, "It should be active broker.")
		s0.Shutdown()
		assert.Equal(t, s0.IsShutdown(), true, "It should not be active broker.")
	})

}
