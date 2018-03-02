package jocko_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	ti "github.com/mitchellh/go-testing-interface"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	topic = "test_topic"
)

func TestServer(t *testing.T) {
	s1, teardown1 := jocko.NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.BootstrapExpect = 3
		cfg.Bootstrap = true
	}, nil)
	ctx1, cancel1 := context.WithCancel((context.Background()))
	defer cancel1()
	err := s1.Start(ctx1)
	require.NoError(t, err)
	defer teardown1()
	// TODO: mv close into teardown
	defer s1.Close()

	s2, teardown2 := jocko.NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
	}, nil)
	ctx2, cancel2 := context.WithCancel((context.Background()))
	defer cancel2()
	err = s2.Start(ctx2)
	require.NoError(t, err)
	defer teardown2()
	defer s2.Close()

	s3, teardown3 := jocko.NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
	}, nil)
	ctx3, cancel3 := context.WithCancel((context.Background()))
	defer cancel3()
	err = s3.Start(ctx3)
	require.NoError(t, err)
	defer teardown3()
	defer s3.Close()

	jocko.TestJoin(t, s1, s2, s3)
	controller, others := jocko.WaitForLeader(t, s1, s2, s3)

	err = createTopic(t, controller, others...)
	require.NoError(t, err)

	t.Run("Produce and consume", func(t *testing.T) {
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_0_0
		config.ChannelBufferSize = 1
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10

		brokers := []string{controller.Addr().String()}
		// for _, o := range others {
		// 	brokers = append(brokers, o.Addr().String())
		// }

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

		fmt.Printf("closing controller: %d/%s\n", controller.ID(), controller.Addr().String())
		switch controller {
		case s1:
			cancel1()
		case s2:
			cancel2()
		case s3:
			cancel3()
		}
		controller.Close()

		time.Sleep(3 * time.Second)

		controller, others = jocko.WaitForLeader(t, others...)
		fmt.Printf("elected controller: %d/%s\n", controller.ID(), controller.Addr().String())
		// TODO: kill the controller, try consuming from one of the replicas
		brokers = []string{controller.Addr().String()}
		client, err := sarama.NewClient(brokers, config)
		require.NoError(t, err)
		spew.Dump("test brokers:", client.Brokers())
		consumer, err = sarama.NewConsumer(brokers, config)
		require.NoError(t, err)
		cPartition, err = consumer.ConsumePartition(topic, pPartition, 0)
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

func BenchmarkServer(b *testing.B) {
	ctx, cancel := context.WithCancel((context.Background()))
	defer cancel()
	srv, teardown := jocko.NewTestServer(b, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer teardown()
	err := srv.Start(ctx)
	require.NoError(b, err)

	err = createTopic(b, srv)
	require.NoError(b, err)

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	brokers := []string{srv.Addr().String()}

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

func createTopic(t ti.T, s1 *jocko.Server, other ...*jocko.Server) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", s1.Addr().String())
	require.NoError(t, err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	require.NoError(t, err)
	client := jocko.NewClient(conn)

	assignment := []int32{s1.ID()}
	for _, o := range other {
		assignment = append(assignment, o.ID())
	}

	_, err = client.CreateTopics("testclient", &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(1),
			ReplicationFactor: int16(3),
			ReplicaAssignment: map[int32][]int32{
				0: assignment,
			},
			Configs: map[string]string{
				"config_key": "config_val",
			},
		}},
	})

	return err
}
