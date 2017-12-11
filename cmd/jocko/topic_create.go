package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

type TopicCreateCommand struct{}

func (c *TopicCreateCommand) Help() string {
	helpText := `
Usage: jocko topic create [options] <topic>
  Create is used to create new topics.
General Options:
  ` + generalOptionsUsage() + `
Deployments Options:
  -partitions
    Output the deployments in a JSON format.
  -addr
    Format and display deployments using a Go template.
  -replication-factor
    Display the latest deployment only.
`
	return strings.TrimSpace(helpText)
}

func (c *TopicCreateCommand) Synopsis() string {
	return "Create a topic."
}

func (c *TopicCreateCommand) Run(args []string) int {
	var flagAddr, topic string
	var partitions, replicationFactor int

	f := flag.NewFlagSet("topic create", flag.ContinueOnError)
	f.StringVar(&flagAddr, "addr", "", "")
	f.StringVar(&topic, "topic", "", "")
	f.IntVar(&partitions, "partitions", 0, "")
	f.IntVar(&replicationFactor, "replication-factor", 0, "")

	addr, err := net.ResolveTCPAddr("tcp", flagAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down store: %v\n", err)
		os.Exit(1)
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error connecting to broker: %v\n", err)
		os.Exit(1)
	}

	client := server.NewClient(conn)
	resp, err := client.CreateTopics("cmd/createtopic", &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replicationFactor),
			ReplicaAssignment: nil,
			Configs:           nil,
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with request to broker: %v\n", err)
		os.Exit(1)
	}
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() {
			err := protocol.Errs[topicErrCode.ErrorCode]
			fmt.Fprintf(os.Stderr, "error code: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("created topic: %v\n", topic)

	return 0
}
