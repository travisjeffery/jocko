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

func TestKafkaConsumerGroupMultiMemberRebalance(t *testing.T) {
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

	err = createTopicWithPartitions(t, srv, 4)
	require.NoError(t, err)

	brokers := []string{srv.Addr().String()}
	producerConfig := sarama.NewConfig()
	producerConfig.ClientID = "kafka-consumer-group-rebalance-producer"
	producerConfig.Version = sarama.V0_10_0_0
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	require.NoError(t, err)
	defer producer.Close()

	groupID := "consumer-group-rebalance-e2e"
	consumerA, err := cluster.NewConsumer(brokers, groupID, []string{topic}, consumerGroupConfig("consumer-group-member-a"))
	require.NoError(t, err)
	defer consumerA.Close()

	waitForConsumerPartitions(t, consumerA, 4, 10*time.Second)

	consumerB, err := cluster.NewConsumer(brokers, groupID, []string{topic}, consumerGroupConfig("consumer-group-member-b"))
	require.NoError(t, err)
	defer consumerB.Close()

	assignA, assignB := waitForBalancedConsumers(t, consumerA, consumerB, 4, 10*time.Second)

	firstBatch := producePartitionMessages(t, producer, "first-rebalance", 4)
	firstMessages := collectConsumerMessages(t, map[string]*cluster.Consumer{
		"a": consumerA,
		"b": consumerB,
	}, firstBatch, 10*time.Second)
	assertMessagesMatchAssignments(t, firstMessages, map[string]map[int32]bool{
		"a": assignA,
		"b": assignB,
	})
	commitCollectedMessages(t, firstMessages, map[string]*cluster.Consumer{
		"a": consumerA,
		"b": consumerB,
	})

	require.NoError(t, consumerB.Close())
	waitForConsumerPartitions(t, consumerA, 4, 10*time.Second)

	secondBatch := producePartitionMessages(t, producer, "after-leave", 4)
	secondMessages := collectConsumerMessages(t, map[string]*cluster.Consumer{
		"a": consumerA,
	}, secondBatch, 10*time.Second)
	require.Len(t, secondMessages, 4)
	for _, msg := range secondMessages {
		require.Equal(t, "a", msg.consumer)
	}
}

func consumerGroupConfig(clientID string) *cluster.Config {
	config := cluster.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V0_10_0_0
	config.ChannelBufferSize = 1
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	return config
}

func waitForConsumerPartitions(t *testing.T, consumer *cluster.Consumer, want int, timeout time.Duration) map[int32]bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		partitions := partitionSet(consumer.Subscriptions())
		if len(partitions) == want {
			return partitions
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d partitions, got %v", want, consumer.Subscriptions())
	return nil
}

func waitForBalancedConsumers(t *testing.T, consumerA, consumerB *cluster.Consumer, partitionCount int, timeout time.Duration) (map[int32]bool, map[int32]bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		assignA := partitionSet(consumerA.Subscriptions())
		assignB := partitionSet(consumerB.Subscriptions())
		if len(assignA) > 0 && len(assignB) > 0 && disjoint(assignA, assignB) && len(assignA)+len(assignB) == partitionCount {
			return assignA, assignB
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for balanced consumers: a=%v b=%v", consumerA.Subscriptions(), consumerB.Subscriptions())
	return nil, nil
}

func partitionSet(subscriptions map[string][]int32) map[int32]bool {
	partitions := make(map[int32]bool)
	for _, topicPartitions := range subscriptions {
		for _, partition := range topicPartitions {
			partitions[partition] = true
		}
	}
	return partitions
}

func disjoint(a, b map[int32]bool) bool {
	for partition := range a {
		if b[partition] {
			return false
		}
	}
	return true
}

func producePartitionMessages(t *testing.T, producer sarama.SyncProducer, prefix string, partitionCount int32) map[int32][]byte {
	t.Helper()

	messages := make(map[int32][]byte, partitionCount)
	for partition := int32(0); partition < partitionCount; partition++ {
		value := []byte(fmt.Sprintf("%s-partition-%d", prefix, partition))
		producedPartition, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Value:     sarama.ByteEncoder(value),
		})
		require.NoError(t, err)
		require.Equal(t, partition, producedPartition)
		messages[partition] = value
	}
	return messages
}

type collectedConsumerMessage struct {
	consumer string
	message  *sarama.ConsumerMessage
}

func collectConsumerMessages(t *testing.T, consumers map[string]*cluster.Consumer, expected map[int32][]byte, timeout time.Duration) map[int32]collectedConsumerMessage {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	messages := make(map[int32]collectedConsumerMessage, len(expected))
	for len(messages) < len(expected) {
		select {
		case msg := <-consumers["a"].Messages():
			collectExpectedMessage(t, messages, "a", msg, expected)
		case err := <-consumers["a"].Errors():
			require.NoError(t, err)
		case msg := <-messageChannel(consumers["b"]):
			collectExpectedMessage(t, messages, "b", msg, expected)
		case err := <-errorChannel(consumers["b"]):
			require.NoError(t, err)
		case <-timer.C:
			t.Fatalf("timed out waiting for messages: got %d, want %d", len(messages), len(expected))
		}
	}
	return messages
}

func collectExpectedMessage(t *testing.T, messages map[int32]collectedConsumerMessage, consumer string, msg *sarama.ConsumerMessage, expected map[int32][]byte) {
	t.Helper()

	value, ok := expected[msg.Partition]
	if !ok {
		return
	}
	if _, seen := messages[msg.Partition]; seen {
		return
	}
	require.Equal(t, topic, msg.Topic)
	require.Equal(t, value, msg.Value)
	messages[msg.Partition] = collectedConsumerMessage{consumer: consumer, message: msg}
}

func messageChannel(consumer *cluster.Consumer) <-chan *sarama.ConsumerMessage {
	if consumer == nil {
		return nil
	}
	return consumer.Messages()
}

func errorChannel(consumer *cluster.Consumer) <-chan error {
	if consumer == nil {
		return nil
	}
	return consumer.Errors()
}

func assertMessagesMatchAssignments(t *testing.T, messages map[int32]collectedConsumerMessage, assignments map[string]map[int32]bool) {
	t.Helper()

	require.Len(t, messages, 4)
	for partition, msg := range messages {
		require.True(t, assignments[msg.consumer][partition], "consumer %s received unassigned partition %d", msg.consumer, partition)
	}
}

func commitCollectedMessages(t *testing.T, messages map[int32]collectedConsumerMessage, consumers map[string]*cluster.Consumer) {
	t.Helper()

	for _, msg := range messages {
		consumers[msg.consumer].MarkOffset(msg.message, "")
	}
	for _, consumer := range consumers {
		require.NoError(t, consumer.CommitOffsets())
	}
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
	return createTopicWithPartitions(t, s1, 1, other...)
}

func createTopicWithPartitions(t ti.T, s1 *jocko.Server, partitions int32, other ...*jocko.Server) error {
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
	replicaAssignment := make(map[int32][]int32, partitions)
	for partition := int32(0); partition < partitions; partition++ {
		replicaAssignment[partition] = assignment
	}
	res, err := conn.CreateTopics(&protocol.CreateTopicRequests{
		Timeout: 15 * time.Second,
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: int16(len(assignment)),
			ReplicaAssignment: replicaAssignment,
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
