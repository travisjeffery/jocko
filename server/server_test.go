package server

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
)

func TestNewServer(t *testing.T) {
	dir := os.TempDir()
	os.RemoveAll(dir)

	logs := filepath.Join(dir, "logs")
	assert.NoError(t, os.MkdirAll(logs, 0755))

	data := filepath.Join(dir, "data")
	assert.NoError(t, os.MkdirAll(data, 0755))

	store := broker.New(broker.Options{
		DataDir:  data,
		BindAddr: "localhost:0",
		LogDir:   logs,
	})
	assert.NoError(t, store.Open())
	defer store.Close()

	_, err := store.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	s := New(":8000", store)
	assert.NotNil(t, s)
	assert.NoError(t, s.Start())

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8000")
	assert.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	assert.NoError(t, err)
	defer conn.Close()

	var body protocol.Body = &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             "test_topic",
			NumPartitions:     int32(8),
			ReplicationFactor: int16(2),
			ReplicaAssignment: map[int32][]int32{
				0: []int32{1, 2},
			},
			Configs: map[string]string{
				"config_key": "config_val",
			},
		}},
	}
	var req protocol.Encoder = &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      "test_client",
		Body:          body,
	}

	b, err := protocol.Encode(req)
	assert.NoError(t, err)

	_, err = conn.Write(b)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	_, err = io.CopyN(buf, conn, 8)
	assert.NoError(t, err)

	var header protocol.Response
	protocol.Decode(buf.Bytes(), &header)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	assert.NoError(t, err)

	createTopicsResponse := &protocol.CreateTopicsResponse{}
	err = protocol.Decode(buf.Bytes(), createTopicsResponse)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(createTopicsResponse.TopicErrorCodes))
	assert.Equal(t, "test_topic", createTopicsResponse.TopicErrorCodes[0].Topic)
	assert.Equal(t, protocol.ErrNone, createTopicsResponse.TopicErrorCodes[0].ErrorCode)

	body = &protocol.MetadataRequest{
		Topics: []string{"test_topic"},
	}
	req = &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      "test_client",
		Body:          body,
	}
	b, err = protocol.Encode(req)
	assert.NoError(t, err)

	_, err = conn.Write(b)
	assert.NoError(t, err)

	buf.Reset()
	_, err = io.CopyN(buf, conn, 8)
	assert.NoError(t, err)
	protocol.Decode(buf.Bytes(), &header)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	assert.NoError(t, err)

	metadataResponse := &protocol.MetadataResponse{}
	err = protocol.Decode(buf.Bytes(), metadataResponse)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(metadataResponse.Brokers))
	assert.Equal(t, "test_topic", metadataResponse.TopicMetadata[0].Topic)

}
