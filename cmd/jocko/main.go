package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/prometheus"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/jocko/log"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	cli       = kingpin.New("jocko", "Jocko, Go implementation of Kafka")
	debugLogs = cli.Flag("debug", "Enable debug logs").Default("false").Bool()

	brokerCmd            = cli.Command("broker", "Run a Jocko broker")
	brokerCmdRaftAddr    = brokerCmd.Flag("raft-addr", "Address for Raft to bind and advertise on").Default("127.0.0.1:9093").String()
	brokerCmdLogDir      = brokerCmd.Flag("log-dir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").String()
	brokerCmdBrokerAddr  = brokerCmd.Flag("broker-addr", "Address for broker to bind on").Default("0.0.0.0:9092").String()
	brokerCmdSerfAddr    = brokerCmd.Flag("serf-addr", "Address for Serf to bind on").Default("0.0.0.0:9094").String()
	brokerCmdHTTPAddr    = brokerCmd.Flag("http-addr", "Address for HTTP handlers to serve metrics on, like Prometheus").Default(":9095").String()
	brokerCmdSerfMembers = brokerCmd.Flag("serf-members", "List of existing Serf members").Strings()
	brokerCmdBrokerID    = brokerCmd.Flag("id", "Broker ID").Int32()

	topicCmd                     = cli.Command("topic", "Manage topics")
	createTopicCmd               = topicCmd.Command("create", "Create a topic")
	createTopicBrokerAddr        = createTopicCmd.Flag("broker-addr", "Address for Broker to bind on").Default("0.0.0.0:9092").String()
	createTopicTopic             = createTopicCmd.Flag("topic", "Name of topic to create").String()
	createTopicPartitions        = createTopicCmd.Flag("partitions", "Number of partitions").Default("1").Int32()
	createTopicReplicationFactor = createTopicCmd.Flag("replication-factor", "Replication factor").Default("1").Int16()
)

func main() {
	cmd := kingpin.MustParse(cli.Parse(os.Args[1:]))

	var logger log.Logger = log.New()

	switch cmd {
	case brokerCmd.FullCommand():
		os.Exit(cmdBrokers(logger))
	case createTopicCmd.FullCommand():
		os.Exit(cmdCreateTopic(logger))
	}
}

func cmdBrokers(logger log.Logger) int {
	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr(*brokerCmdSerfAddr),
		serf.InitMembers(*brokerCmdSerfMembers),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting serf: %v\n", err)
		os.Exit(1)
	}

	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(*brokerCmdLogDir),
		raft.Addr(*brokerCmdRaftAddr),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting raft: %v\n", err)
		os.Exit(1)
	}

	store, err := broker.New(*brokerCmdBrokerID,
		broker.LogDir(*brokerCmdLogDir),
		broker.Logger(logger),
		broker.Addr(*brokerCmdBrokerAddr),
		broker.Serf(serf),
		broker.Raft(raft),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(*brokerCmdBrokerAddr, store, *brokerCmdHTTPAddr, prometheus.NewMetrics(), logger)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error starting server: %v\n", err)
		os.Exit(1)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := store.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down store: %v\n", err)
		os.Exit(1)
	}

	return 0
}

func cmdCreateTopic(logger log.Logger) int {
	addr, err := net.ResolveTCPAddr("tcp", *createTopicBrokerAddr)
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
	resp, err := client.CreateTopic("cmd/createtopic", &protocol.CreateTopicRequest{
		Topic:             *createTopicTopic,
		NumPartitions:     *createTopicPartitions,
		ReplicationFactor: *createTopicReplicationFactor,
		ReplicaAssignment: nil,
		Configs:           nil,
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

	fmt.Printf("created topic: %v\n", *createTopicTopic)

	return 0
}
