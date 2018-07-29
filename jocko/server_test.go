package jocko_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/hashicorp/consul/testutil/retry"
	ti "github.com/mitchellh/go-testing-interface"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	topic = "test_topic"
)

func init() {
	log.SetLevel("debug")
}

func TestProduceConsume(t *testing.T) {
	sarama.Logger = log.NewStdLogger(log.New(log.DebugLevel, "server_test: sarama: "))

	s1, dir1 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
	}, nil)
	ctx1, cancel1 := context.WithCancel((context.Background()))
	defer cancel1()
	err := s1.Start(ctx1)
	require.NoError(t, err)
	defer os.RemoveAll(dir1)
	// TODO: mv close into teardown
	defer s1.Shutdown()

	jocko.WaitForLeader(t, s1)

	s2, dir2 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	ctx2, cancel2 := context.WithCancel((context.Background()))
	defer cancel2()
	err = s2.Start(ctx2)
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	s3, dir3 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	ctx3, cancel3 := context.WithCancel((context.Background()))
	defer cancel3()
	err = s3.Start(ctx3)
	require.NoError(t, err)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	jocko.TestJoin(t, s1, s2, s3)
	controller, others := jocko.WaitForLeader(t, s1, s2, s3)

	err = createTopic(t, controller, others...)
	require.NoError(t, err)

	// give raft enough time to register the topic
	time.Sleep(500 * time.Millisecond)

	config := sarama.NewConfig()
	config.ClientID = "produce-consume-test"
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	brokers := []string{controller.Addr().String()}
	for _, o := range others {
		brokers = append(brokers, o.Addr().String())
	}

	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if 3 != len(client.Brokers()) {
			r.Fatalf("client didn't find the right number of brokers: %d", len(client.Brokers()))
		}
	})

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

	switch controller {
	case s1:
		cancel1()
	case s2:
		cancel2()
	case s3:
		cancel3()
	}
	controller.Leave()
	controller.Shutdown()

	time.Sleep(3 * time.Second)

	controller, others = jocko.WaitForLeader(t, others...)

	time.Sleep(time.Second)

	brokers = []string{controller.Addr().String()}
	for _, o := range others {
		brokers = append(brokers, o.Addr().String())
	}

	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if 2 != len(client.Brokers()) {
			r.Fatalf("client didn't find the right number of brokers: %d", len(client.Brokers()))
		}
	})

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
}

func TestConsumerGroup(t *testing.T) {
	t.Skip()

	s1, dir1 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
	}, nil)
	ctx1, cancel1 := context.WithCancel((context.Background()))
	defer cancel1()
	err := s1.Start(ctx1)
	require.NoError(t, err)
	defer os.RemoveAll(dir1)
	// TODO: mv close into dir
	defer s1.Shutdown()

	jocko.WaitForLeader(t, s1)

	s2, dir2 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	ctx2, cancel2 := context.WithCancel((context.Background()))
	defer cancel2()
	err = s2.Start(ctx2)
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	s3, dir3 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	ctx3, cancel3 := context.WithCancel((context.Background()))
	defer cancel3()
	err = s3.Start(ctx3)
	require.NoError(t, err)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	jocko.TestJoin(t, s1, s2, s3)
	controller, others := jocko.WaitForLeader(t, s1, s2, s3)

	err = createTopic(t, controller, others...)
	require.NoError(t, err)

	// give raft enough time to register the topic
	time.Sleep(500 * time.Millisecond)

	config := cluster.NewConfig()
	config.ClientID = "consumer-group-test"
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	brokers := []string{controller.Addr().String()}
	for _, o := range others {
		brokers = append(brokers, o.Addr().String())
	}

	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, &config.Config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if 3 != len(client.Brokers()) {
			r.Fatalf("client didn't find the right number of brokers: got %d, want %d", len(client.Brokers()), 3)
		}
	})

	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
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

	consumer, err := cluster.NewConsumer(brokers, "consumer-group", []string{topic}, config)
	if err != nil {
		panic(err)
	}
	select {
	case msg := <-consumer.Messages():
		require.Equal(t, msg.Offset, offset)
		require.Equal(t, pPartition, msg.Partition)
		require.Equal(t, topic, msg.Topic)
		require.Equal(t, 0, bytes.Compare(bValue, msg.Value))
	case err := <-consumer.Errors():
		require.NoError(t, err)
	}

	switch controller {
	case s1:
		cancel1()
	case s2:
		cancel2()
	case s3:
		cancel3()
	}
	controller.Shutdown()

	time.Sleep(3 * time.Second)

	controller, others = jocko.WaitForLeader(t, others...)

	time.Sleep(time.Second)

	brokers = []string{controller.Addr().String()}
	for _, o := range others {
		brokers = append(brokers, o.Addr().String())
	}

	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, &config.Config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if 2 != len(client.Brokers()) {
			r.Fatalf("client didn't find the right number of brokers: %d", len(client.Brokers()))
		}
	})

	consumer, err = cluster.NewConsumer(brokers, "consumer-group", []string{topic}, config)
	if err != nil {
		panic(err)
	}
	select {
	case msg, more := <-consumer.Messages():
		if !more {
			break
		}
		// require.Equal(t, msg.Offset, offset)
		// require.Equal(t, pPartition, msg.Partition)
		require.Equal(t, topic, msg.Topic)
		// require.Equal(t, 0, bytes.Compare(bValue, msg.Value))
	case err, more := <-consumer.Errors():
		if !more {
			break
		}
		require.NoError(t, err)
	}

}

func BenchmarkServer(b *testing.B) {
	ctx, cancel := context.WithCancel((context.Background()))
	defer cancel()
	srv, dir := jocko.NewTestServer(b, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer os.RemoveAll(dir)
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

	b.Run("Produce", func(b *testing.B) {
		msgCount = b.N

		for i := 0; i < b.N; i++ {
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: msgValue,
			})
			require.NoError(b, err)
		}
	})

	b.Run("Consume", func(b *testing.B) {
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
	d := &jocko.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  t.Name(),
	}
	conn, err := d.Dial("tcp", s1.Addr().String())
	if err != nil {
		return err
	}
	assignment := []int32{s1.ID()}
	for _, o := range other {
		assignment = append(assignment, o.ID())
	}
	_, err = conn.CreateTopics(&protocol.CreateTopicRequests{
		Timeout: 15 * time.Second,
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(1),
			ReplicationFactor: int16(3),
			ReplicaAssignment: map[int32][]int32{
				0: assignment,
			},
			Configs: map[string]*string{
				"config_key": strPointer("config_val"),
			},
		}},
	})
	return err
}

func strPointer(v string) *string {
	return &v
}
