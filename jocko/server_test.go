package jocko_test

import (
	"bytes"
	"context"
	"fmt"
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

func TestKafkaClientProduceConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, dir := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer os.RemoveAll(dir)

	err := srv.Start(ctx)
	require.NoError(t, err)
	defer srv.Shutdown()

	jocko.WaitForLeader(t, srv)

	err = createTopic(t, srv)
	require.NoError(t, err)

	config := sarama.NewConfig()
	config.ClientID = "kafka-client-e2e-test"
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	brokers := []string{srv.Addr().String()}
	retry.Run(t, func(r *retry.R) {
		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		defer client.Close()

		if got := len(client.Brokers()); got != 1 {
			r.Fatalf("client found wrong broker count: %d", got)
		}

		partitions, err := client.Partitions(topic)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if len(partitions) != 1 || partitions[0] != 0 {
			r.Fatalf("client found wrong partitions: %v", partitions)
		}
	})

	producer, err := sarama.NewSyncProducer(brokers, config)
	require.NoError(t, err)
	defer producer.Close()

	wantValue := []byte("Hello from Sarama!")
	producedPartition, producedOffset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(wantValue),
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), producedPartition)

	consumer, err := sarama.NewConsumer(brokers, config)
	require.NoError(t, err)
	defer consumer.Close()

	partition, err := consumer.ConsumePartition(topic, producedPartition, producedOffset)
	require.NoError(t, err)
	defer partition.Close()

	select {
	case msg := <-partition.Messages():
		require.Equal(t, producedOffset, msg.Offset)
		require.Equal(t, producedPartition, msg.Partition)
		require.Equal(t, topic, msg.Topic)
		require.Equal(t, wantValue, msg.Value)
	case err := <-partition.Errors():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting to consume produced message")
	}
}

func TestKafkaConsumerGroupProduceConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, dir := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
		cfg.OffsetsTopicReplicationFactor = 1
	}, nil)
	defer os.RemoveAll(dir)

	err := srv.Start(ctx)
	require.NoError(t, err)
	defer srv.Shutdown()

	jocko.WaitForLeader(t, srv)

	err = createTopic(t, srv)
	require.NoError(t, err)

	config := cluster.NewConfig()
	config.ClientID = "kafka-consumer-group-e2e-test"
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	brokers := []string{srv.Addr().String()}
	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
	require.NoError(t, err)
	defer producer.Close()

	firstValue := []byte("first consumer group message")
	firstPartition, firstOffset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(firstValue),
	})
	require.NoError(t, err)

	groupID := "consumer-group-e2e"
	consumer, err := cluster.NewConsumer(brokers, groupID, []string{topic}, config)
	require.NoError(t, err)

	firstMsg := waitForClusterMessage(t, consumer, 10*time.Second)
	require.Equal(t, firstOffset, firstMsg.Offset)
	require.Equal(t, firstPartition, firstMsg.Partition)
	require.Equal(t, topic, firstMsg.Topic)
	require.Equal(t, firstValue, firstMsg.Value)

	consumer.MarkOffset(firstMsg, "first-done")
	require.NoError(t, consumer.CommitOffsets())
	require.NoError(t, consumer.Close())

	secondValue := []byte("second consumer group message")
	secondPartition, secondOffset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(secondValue),
	})
	require.NoError(t, err)
	require.Equal(t, firstPartition, secondPartition)
	require.Equal(t, firstOffset+1, secondOffset)

	consumer, err = cluster.NewConsumer(brokers, groupID, []string{topic}, config)
	require.NoError(t, err)
	defer consumer.Close()

	secondMsg := waitForClusterMessage(t, consumer, 10*time.Second)
	require.Equal(t, secondOffset, secondMsg.Offset)
	require.Equal(t, secondPartition, secondMsg.Partition)
	require.Equal(t, topic, secondMsg.Topic)
	require.Equal(t, secondValue, secondMsg.Value)
}

func waitForClusterMessage(t *testing.T, consumer *cluster.Consumer, timeout time.Duration) *sarama.ConsumerMessage {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg := <-consumer.Messages():
			return msg
		case err := <-consumer.Errors():
			require.NoError(t, err)
		case <-timer.C:
			t.Fatal("timed out waiting for consumer group message")
		}
	}
}

func TestProduceConsume(t *testing.T) {
	t.Skip()

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
	res, err := conn.CreateTopics(&protocol.CreateTopicRequests{
		Timeout: 15 * time.Second,
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(1),
			ReplicationFactor: int16(len(assignment)),
			ReplicaAssignment: map[int32][]int32{
				0: assignment,
			},
			Configs: map[string]*string{
				"config_key": strPointer("config_val"),
			},
		}},
	})
	if err != nil {
		return err
	}
	for _, topicErr := range res.TopicErrorCodes {
		if topicErr.ErrorCode != protocol.ErrNone.Code() {
			return fmt.Errorf("create topic %q failed: %s", topicErr.Topic, protocol.Errs[topicErr.ErrorCode])
		}
	}
	return nil
}

func strPointer(v string) *string {
	return &v
}
