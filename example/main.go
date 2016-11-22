package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type check struct {
	partition int32
	offset    int64
	message   string
}

const (
	topic        = "test_topic"
	messageCount = 15
)

var (
	logDir   = kingpin.Flag("logdir", "A comma separated list of directories under which to store log files").Default("logdir").String()
	tcpaddr  = kingpin.Flag("tcpaddr", "HTTP Address to listen on").Default(":8000").String()
	raftDir  = kingpin.Flag("raftdir", "Directory for raft to store data").Default("raftdir").String()
	raftAddr = kingpin.Flag("raftaddr", "Address for Raft to bind on").Default(":4000").String()
	brokerID = kingpin.Flag("id", "Broker ID").Int()
)

func main() {
	kingpin.Parse()

	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "jocko")

	store := broker.New(broker.Options{
		DataDir:  *raftDir,
		RaftAddr: *raftAddr,
		TCPAddr:  *tcpaddr,
		LogDir:   *logDir,
		ID:       *brokerID,
		Logger:   logger,
	})
	if err := store.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "Error opening raft store: %s\n", err)
		os.Exit(1)
	}
	server := server.New(*tcpaddr, store, logger)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	clientID := "test_client"
	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpaddr)
	if err != nil {
		panic(err)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	defer conn.Close()

	_, err = store.WaitForLeader(10 * time.Second)
	if err != nil {
		panic(err)
	}

	numPartitions := int32(8)

	var body protocol.Body = &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: int16(0),
			ReplicaAssignment: nil,
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

	_, err = conn.Write(b)

	buf := new(bytes.Buffer)
	_, err = io.CopyN(buf, conn, 8)

	var header protocol.Response
	protocol.Decode(buf.Bytes(), &header)

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))

	createTopicsResponse := &protocol.CreateTopicsResponse{}
	err = protocol.Decode(buf.Bytes(), createTopicsResponse)

	config := sarama.NewConfig()
	config.ChannelBufferSize = 1
	config.Version = sarama.V0_10_0_1

	brokers := []string{*tcpaddr}
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
	if err = store.DeleteTopic(topic); err != nil {
		panic(err)
	}

	fmt.Printf("producer and consumer worked! %d messages ok\n", totalChecked)
}
