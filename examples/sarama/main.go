package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
)

type check struct {
	partition int32
	offset    int64
	message   string
}

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(8)
	brokerAddr    = "127.0.0.1:9092"
	raftAddr      = "127.0.0.1:9093"
	serfAddr      = "127.0.0.1:9094"
	logDir        = "logdir"
	brokerID      = 0
)

func main() {
	defer setup()()

	config := sarama.NewConfig()
	config.ChannelBufferSize = 1
	config.Version = sarama.V0_10_0_1
	config.Producer.Return.Successes = true

	brokers := []string{brokerAddr}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	pmap := make(map[int32][]check)

	for i := 0; i < messageCount; i++ {
		message := fmt.Sprintf("Hello from Jocko #%d!", i)
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			panic(err)
		}
		pmap[partition] = append(pmap[partition], check{
			partition: partition,
			offset:    offset,
			message:   message,
		})
	}
	if err = producer.Close(); err != nil {
		panic(err)
	}

	var totalChecked int
	for partitionID := range pmap {
		checked := 0
		consumer, err := sarama.NewConsumer(brokers, config)
		if err != nil {
			panic(err)
		}
		partition, err := consumer.ConsumePartition(topic, partitionID, 0)
		if err != nil {
			panic(err)
		}
		i := 0
		for msg := range partition.Messages() {
			fmt.Printf("msg partition [%d] offset [%d]\n", msg.Partition, msg.Offset)
			check := pmap[partitionID][i]
			if string(msg.Value) != check.message {
				log.Fatalf("msg value not equal! partition %d, offset: %d!\n", msg.Partition, msg.Offset)
			}
			if msg.Offset != check.offset {
				log.Fatalf("msg offset not equal! partition %d, offset: %d!\n", msg.Partition, msg.Offset)
			}
			log.Printf("msg is ok! partition: %d, offset: %d\n", msg.Partition, msg.Offset)
			i++
			checked++
			fmt.Printf("i: %d, len: %d\n", i, len(pmap[partitionID]))
			if i == len(pmap[partitionID]) {
				totalChecked += checked
				fmt.Println("checked partition:", partitionID)
				if err = consumer.Close(); err != nil {
					panic(err)
				}
				break
			} else {
				fmt.Println("still checking partition:", partitionID)
			}
		}
	}
	fmt.Printf("producer and consumer worked! %d messages ok\n", totalChecked)
}

func setup() func() {
	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "jocko")

	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr(serfAddr),
	)

	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(logDir),
		raft.Addr(raftAddr),
	)

	store, err := broker.New(brokerID,
		broker.LogDir(logDir),
		broker.Logger(logger),
		broker.Addr(brokerAddr),
		broker.Serf(serf),
		broker.Raft(raft),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening raft store: %s\n", err)
		os.Exit(1)
	}
	server := server.New(brokerAddr, store, logger)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	if _, err := store.WaitForLeader(10 * time.Second); err != nil {
		panic(err)
	}

	// creating/deleting topic directly since Sarama doesn't support it
	if err := store.CreateTopic(topic, numPartitions, 1); err != protocol.ErrNone {
		panic(err)
	}

	return func() {
		os.RemoveAll(logDir)
	}
}
