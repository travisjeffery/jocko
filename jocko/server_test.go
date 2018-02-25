package jocko_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/testutil"
)

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(1)
)

func TestBroker(t *testing.T) {
	bConfig, shutdown := setup(t)
	defer shutdown()

	t.Run("Sarama", func(t *testing.T) {
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_0_0
		config.ChannelBufferSize = 1
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10
		brokers := []string{bConfig.Addr}

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
	bConfig, shutdown := setup(b)
	defer shutdown()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	brokers := []string{bConfig.Addr}

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

func setup(t require.TestingT) (*config.BrokerConfig, func()) {
	dataDir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	logger := log.New()

	ports := dynaport.Get(3)
	dir, cfg := testutil.TestConfig(t.(*testing.T))
	defer os.RemoveAll(dir)
	cfg.Bootstrap = true
	cfg.BootstrapExpect = 1
	cfg.StartAsLeader = true

	broker, err := jocko.NewBroker(cfg, logger)
	if err != nil {
		panic(err)
	}
	require.NoError(t, err)

	ctx, cancel := context.WithCancel((context.Background()))
	srv := jocko.NewServer(&jocko.ServerConfig{BrokerAddr: cfg.Addr, HTTPAddr: fmt.Sprintf("127.0.0.1:%d", ports[2])}, broker, nil, logger)
	require.NotNil(t, srv)
	require.NoError(t, srv.Start(ctx))

	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.Addr)
	require.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	require.NoError(t, err)

	client := jocko.NewClient(conn)
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

	return cfg, func() {
		defer func() {
			logger.Info("removing data dir")
			os.RemoveAll(dataDir)
		}()
		cancel()
		srv.Close()
		broker.Shutdown()
	}
}
