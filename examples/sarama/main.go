package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Shopify/sarama"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/broker/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
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
	httpAddr      = "127.0.0.1:9095"
	logDir        = "logdir"
	brokerID      = 0
)

func main() {
	logger := log.New()
	logger = logger.With(log.String("example", "sarama"))
	defer setup(logger)()

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
				logger.Fatal("msg values not equal", log.Int32("partition", msg.Partition), log.Int64("offset", msg.Offset))
			}
			if msg.Offset != check.offset {
				logger.Fatal("msg offsets not equal", log.Int32("partition", msg.Partition), log.Int64("offset", msg.Offset))
			}
			logger.Info("msg is ok", log.Int32("partition", msg.Partition), log.Int64("offset", msg.Offset))
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

func setup(logger log.Logger) func() {
	ports := dynaport.Get(3)
	config := &config.Config{
		ID:              brokerID,
		Bootstrap:       true,
		BootstrapExpect: 1,
		StartAsLeader:   true,
		DataDir:         logDir + "/logs",
		DevMode:         true,
		Addr:            fmt.Sprintf("127.0.0.1:%d", ports[1]),
		RaftAddr:        fmt.Sprintf("127.0.0.1:%d", ports[2]),
	}
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.BindPort = ports[1]
	config.SerfLANConfig.MemberlistConfig.AdvertiseAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.AdvertisePort = ports[1]
	config.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	broker, err := broker.New(config, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(&server.Config{BrokerAddr: brokerAddr, HTTPAddr: httpAddr}, broker, mock.NewMetrics(), logger)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "failed starting server: %v\n", err)
		os.Exit(1)
	}

	addr, err := net.ResolveTCPAddr("tcp", brokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to resolve addr: %v\n", err)
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to broker: %v\n", err)
		os.Exit(1)
	}

	client := server.NewClient(conn)
	resp, err := client.CreateTopics("cmd/createtopic", &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
			ReplicaAssignment: nil,
			Configs:           nil,
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed with request to broker: %v\n", err)
		os.Exit(1)
	}
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() && topicErrCode.ErrorCode != protocol.ErrTopicAlreadyExists.Code() {
			err := protocol.Errs[topicErrCode.ErrorCode]
			fmt.Fprintf(os.Stderr, "error code: %v\n", err)
			os.Exit(1)
		}
	}

	return func() {
		os.RemoveAll(logDir)
	}
}
