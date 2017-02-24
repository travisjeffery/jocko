package broker

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/jocko/testutil"
)

func TestBroker_Replicate(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "replicate_test")
	defer os.RemoveAll(dataDir)

	s0 := testServer(t, 0)
	defer s0.Shutdown()

	s0.WaitForLeader(10 * time.Second)

	srv := server.New(s0.brokerAddr, s0, logger)
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

	var p *jocko.Partition
	testutil.WaitForResult(func() (bool, error) {
		p, err = s0.Partition("test", 0)
		if err != nil {
			return false, fmt.Errorf("error in fetching partition: %v", err)
		}
		if p == nil {
			return false, fmt.Errorf("partition not found")
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	replicator := newReplicator(p, 0,
		ReplicatorMinBytes(5),
		ReplicatorMaxWaitTime(int32(250*time.Millisecond)),
		ReplicatorProxy(server.NewProxy(p.Conn)))
	assert.NoError(t, err)
	defer replicator.close()

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
}
