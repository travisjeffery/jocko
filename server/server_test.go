package server_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
)

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(1)
)

func TestBroker(t *testing.T) {
	_, teardown := setup(t)

	t.Run("Sarama", func(t *testing.T) {
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_0_0
		config.ChannelBufferSize = 1
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10
		brokers := []string{"127.0.0.1:8000"}

		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			panic(err)
		}

		bValue := []byte("Hello from Jocko!")
		msgValue := sarama.ByteEncoder(bValue)
		pPartition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: msgValue,
		})
		assert.NoError(t, err)

		consumer, err := sarama.NewConsumer(brokers, config)
		assert.NoError(t, err)

		cPartition, err := consumer.ConsumePartition(topic, pPartition, 0)
		assert.NoError(t, err)

		select {
		case msg := <-cPartition.Messages():
			assert.Equal(t, msg.Offset, offset)
			assert.Equal(t, pPartition, msg.Partition)
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, 0, bytes.Compare(bValue, msg.Value))
		case err := <-cPartition.Errors():
			assert.NoError(t, err)
		}
	})

	teardown()
}

func setup(t *testing.T) (*net.TCPConn, func()) {
	dataDir, err := ioutil.TempDir("", "server_test")
	assert.NoError(t, err)

	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "jocko/servertest")
	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr("127.0.0.1:8002"),
	)
	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(dataDir),
		raft.Addr("127.0.0.1:8001"),
	)
	store, err := broker.New(0,
		broker.LogDir(dataDir),
		broker.Addr("127.0.0.1:8000"),
		broker.Raft(raft),
		broker.Serf(serf),
		broker.Logger(logger))
	assert.NoError(t, err)

	_, err = store.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	srv := server.New(":8000", store, logger)
	assert.NotNil(t, srv)
	assert.NoError(t, srv.Start())

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8000")
	assert.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	assert.NoError(t, err)

	createTopic(t, conn)

	return conn, func() {
		conn.Close()
		srv.Close()
		store.Shutdown()
		os.RemoveAll(dataDir)
	}
}
func createTopic(t *testing.T, conn *net.TCPConn) {
	buf := new(bytes.Buffer)
	var header protocol.Response
	var body protocol.Body

	body = &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(1),
			ReplicationFactor: int16(1),
			ReplicaAssignment: map[int32][]int32{
				0: []int32{0, 1},
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

	_, err = io.CopyN(buf, conn, 8)
	assert.NoError(t, err)

	protocol.Decode(buf.Bytes(), &header)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	assert.NoError(t, err)

	createTopicsResponse := &protocol.CreateTopicsResponse{}
	err = protocol.Decode(buf.Bytes(), createTopicsResponse)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(createTopicsResponse.TopicErrorCodes))
	assert.Equal(t, topic, createTopicsResponse.TopicErrorCodes[0].Topic)
	assert.Equal(t, protocol.ErrNone, createTopicsResponse.TopicErrorCodes[0].ErrorCode)
}
