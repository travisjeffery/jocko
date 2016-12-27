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
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/simplelog"
)

const (
	clientID = "test_client"
)

func TestNewServer(t *testing.T) {
	dir := os.TempDir()
	defer os.RemoveAll(dir)

	logs := filepath.Join(dir, "logs")
	assert.NoError(t, os.MkdirAll(logs, 0755))

	data := filepath.Join(dir, "data")
	assert.NoError(t, os.MkdirAll(data, 0755))

	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "jocko/servertest")
	store, err := broker.New(0,
		broker.OptionDataDir(data),
		broker.OptionLogDir(logs),
		broker.OptionRaftAddr("localhost:6000"),
		broker.OptionTCPAddr("localhost:8000"),
		broker.OptionLogger(logger))
	assert.NoError(t, err)
	defer store.Shutdown()

	_, err = store.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	s := New(":8000", store, logger)
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
		ClientID:      clientID,
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
		ClientID:      clientID,
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

	m0 := commitlog.NewMessage([]byte("Hello world!"))
	ms := commitlog.NewMessageSet(0, m0)
	body = &protocol.ProduceRequest{
		TopicData: []*protocol.TopicData{{
			Topic: "test_topic",
			Data: []*protocol.Data{{
				Partition: 0,
				RecordSet: ms,
			}},
		}},
	}
	req = &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      clientID,
		Body:          body,
	}
	b, err = protocol.Encode(req)
	assert.NoError(t, err)
	_, err = conn.Write(b)

	buf.Reset()
	_, err = io.CopyN(buf, conn, 8)
	assert.NoError(t, err)
	protocol.Decode(buf.Bytes(), &header)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	produceResponse := &protocol.ProduceResponses{}
	err = protocol.Decode(buf.Bytes(), produceResponse)
	assert.NoError(t, err)

	assert.Equal(t, req.(*protocol.Request).CorrelationID, header.CorrelationID)
	assert.Equal(t, "test_topic", produceResponse.Responses[0].Topic)
	assert.Equal(t, protocol.ErrNone, produceResponse.Responses[0].PartitionResponses[0].ErrorCode)
	assert.NotEqual(t, 0, produceResponse.Responses[0].PartitionResponses[0].Timestamp)

	body = &protocol.FetchRequest{
		MinBytes: 5,
		Topics: []*protocol.FetchTopic{{
			Topic: "test_topic",
			Partitions: []*protocol.FetchPartition{{
				Partition:   int32(0),
				FetchOffset: int64(0),
				MaxBytes:    int32(5000),
			}},
		}},
	}
	req = &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      clientID,
		Body:          body,
	}
	b, err = protocol.Encode(req)
	assert.NoError(t, err)
	_, err = conn.Write(b)
	assert.NoError(t, err)

	buf.Reset()
	_, err = io.CopyN(buf, conn, 8)
	assert.NoError(t, err)
	err = protocol.Decode(buf.Bytes(), &header)
	assert.NoError(t, err)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	fetchResponse := &protocol.FetchResponses{}
	err = protocol.Decode(buf.Bytes(), fetchResponse)
	assert.NoError(t, err)

	recordSet := commitlog.MessageSet(fetchResponse.Responses[0].PartitionResponses[0].RecordSet)
	assert.Equal(t, int64(0), recordSet.Offset())
	assert.Equal(t, []byte(m0), recordSet.Payload())
}
