package server_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/jocko/testutil/mock"
	"github.com/travisjeffery/jocko/zap"
)

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(1)
)

func TestBroker(t *testing.T) {
	teardown := setup(t)
	defer teardown()

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
		require.NoError(t, err)

		consumer, err := sarama.NewConsumer(brokers, config)
		require.NoError(t, err)

		cPartition, err := consumer.ConsumePartition(topic, pPartition, 0)
		require.NoError(t, err)

		select {
		case msg := <-cPartition.Messages():
			require.Equal(t, msg.Offset, offset)
			require.Equal(t, pPartition, msg.Partition)
			require.Equal(t, topic, msg.Topic)
			require.Equal(t, 0, bytes.Compare(bValue, msg.Value))
		case err := <-cPartition.Errors():
			require.NoError(t, err)
		}
	})
}

func BenchmarkBroker(b *testing.B) {
	teardown := setup(b)
	defer teardown()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	brokers := []string{"127.0.0.1:8000"}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	bValue := []byte("Hello from Jocko!")
	msgValue := sarama.ByteEncoder(bValue)

	var msgCount int

	b.Run("Sarama_Produce", func(b *testing.B) {
		msgCount = b.N

		for i := 0; i < b.N; i++ {
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: msgValue,
			})
			require.NoError(b, err)
		}
	})

	b.Run("Sarama_Consume", func(b *testing.B) {
		consumer, err := sarama.NewConsumer(brokers, config)
		require.NoError(b, err)

		cPartition, err := consumer.ConsumePartition(topic, 0, 0)
		require.NoError(b, err)

		for i := 0; i < msgCount; i++ {
			select {
			case msg := <-cPartition.Messages():
				require.Equal(b, topic, msg.Topic)
			case err := <-cPartition.Errors():
				require.NoError(b, err)
			}
		}
	})
}

func setup(t require.TestingT) func() {
	dataDir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)

	logger := zap.New()
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
		broker.Loner(),
		broker.Logger(logger))
	require.NoError(t, err)

	_, err = store.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel((context.Background()))
	srv := server.New(":8000", store, ":8003", mock.NewMetrics(), logger)
	require.NotNil(t, srv)
	require.NoError(t, srv.Start(ctx))

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8000")
	require.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	require.NoError(t, err)
	defer conn.Close()

	client := server.NewClient(conn)
	_, err = client.CreateTopic("testclient", &protocol.CreateTopicRequest{
		Topic:             topic,
		NumPartitions:     int32(1),
		ReplicationFactor: int16(1),
		ReplicaAssignment: map[int32][]int32{
			0: []int32{0, 1},
		},
		Configs: map[string]string{
			"config_key": "config_val",
		},
	})
	require.NoError(t, err)

	return func() {
		cancel()
		srv.Close()
		store.Shutdown()
		os.RemoveAll(dataDir)
	}
}
