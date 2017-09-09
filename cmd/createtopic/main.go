package main

import (
	"fmt"
	"net"

	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerAddr = kingpin.Flag("brokeraddr", "Address for Broker to bind on").Default("0.0.0.0:9092").String()
	topic      = kingpin.Flag("topic", "Name of topic to create").String()
	partitions = kingpin.Flag("partitions", "Number of partitions").Default("1").Int32()
)

func main() {
	kingpin.Parse()

	addr, err := net.ResolveTCPAddr("tcp", *brokerAddr)
	if err != nil {
		panic(err)
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		panic(err)
	}

	client := server.NewClient(conn)

	resp, err := client.CreateTopic("cmd/createtopic", &protocol.CreateTopicRequest{
		Topic:             *topic,
		NumPartitions:     *partitions,
		ReplicationFactor: int16(1),
		ReplicaAssignment: nil,
		Configs:           nil,
	})

	if err != nil {
		panic(err)
	}

	for _, topicErrCode := range resp.TopicErrorCodes {
		msg := "ok"
		if topicErrCode.ErrorCode == 41 {
			msg = "err not controller"
		}
		fmt.Printf("create topic %s: %d\n", topicErrCode.Topic, msg)
	}
}
