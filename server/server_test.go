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
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
)

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(1)
)

func TestBroker(t *testing.T) {
	brokerPort, shutdown := setup(t)
	defer shutdown()

	t.Run("Sarama", func(t *testing.T) {
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_0_0
		config.ChannelBufferSize = 1
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10
		brokers := []string{"127.0.0.1:" + brokerPort}

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
	brokerPort, shutdown := setup(b)
	defer shutdown()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	brokers := []string{"127.0.0.1:" + brokerPort}

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

func setup(t require.TestingT) (string, func()) {
	dataDir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)

	ports := dynaport.GetS(4)
	brokerPort := ports[0]

	logger := log.New()

	raftAddr := "127.0.0.1:" + ports[1]
	brokerAddr := "127.0.0.1:" + brokerPort
	httpAddr := "127.0.0.1:" + ports[3]

	serf, err := serf.New(serf.Config{ID: 0, Addr: "127.0.0.1:" + ports[2], BrokerAddr: brokerAddr, HTTPAddr: httpAddr, RaftAddr: raftAddr}, logger)
	if err != nil {
		panic(err)
	}
	raft, err := raft.New(raft.Config{
		DataDir:           dataDir + "/raft",
		Bootstrap:         true,
		BootstrapExpect:   1,
		DevMode:           true,
		Addr:              raftAddr,
		ReconcileInterval: time.Second * 5,
	}, serf, logger)
	if err != nil {
		panic(err)
	}
	broker, err := broker.New(broker.Config{ID: 0, DataDir: dataDir + "/logs", DevMode: true, Addr: "127.0.0.1:" + brokerPort}, serf, raft, logger)
	if err != nil {
		panic(err)
	}
	require.NoError(t, err)

	ctx, cancel := context.WithCancel((context.Background()))
	srv := server.New(server.Config{BrokerAddr: ":" + brokerPort, HTTPAddr: httpAddr}, broker, mock.NewMetrics(), logger)
	require.NotNil(t, srv)
	require.NoError(t, srv.Start(ctx))

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:"+brokerPort)
	require.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	require.NoError(t, err)

	client := server.NewClient(conn)
	_, err = client.CreateTopics("testclient", &protocol.CreateTopicRequests{
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
	})
	require.NoError(t, err)
	conn.Close()

	return brokerPort, func() {
		defer func() {
			logger.Info("removing data dir")
			os.RemoveAll(dataDir)
		}()
		cancel()
		srv.Close()
		broker.Shutdown()
	}
}
