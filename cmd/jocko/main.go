package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
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

	topic                  = cli.Command("topic", "Manage topics")
	topicBrokerAddr        = topic.Flag("broker-addr", "Address for Broker to bind on").Default("0.0.0.0:9092").String()
	topicTopic             = topic.Flag("topic", "Name of topic to create").String()
	topicPartitions        = topic.Flag("partitions", "Number of partitions").Default("1").Int32()
	topicReplicationFactor = topic.Flag("replication-factor", "Replication factor").Default("1").Int16()
)

func main() {
	logLevel := simplelog.INFO
	if *debugLogs {
		logLevel = simplelog.DEBUG
	}
	logger := simplelog.New(os.Stdout, logLevel, "jocko")

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case brokerCmd.FullCommand():
		os.Exit(CmdBrokers(logger))

	case topic.FullCommand():
		os.Exit(CmdTopic(logger))
	}
}

func CmdBrokers(logger *simplelog.Logger) int {
	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr(*brokerCmdSerfAddr),
		serf.InitMembers(*brokerCmdSerfMembers),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting serf: %s", err)
		os.Exit(1)
	}

	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(*brokerCmdLogDir),
		raft.Addr(*brokerCmdRaftAddr),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting raft: %s", err)
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
		fmt.Fprintf(os.Stderr, "Error starting broker: %s\n", err)
		os.Exit(1)
	}

	srv := server.New(*brokerCmdBrokerAddr, store, *brokerCmdHTTPAddr, logger)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := store.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "Error shutting down store: %s\n", err)
		os.Exit(1)
	}

	return 0
}

func CmdTopic(logger *simplelog.Logger) int {
	fmt.Fprintf(os.Stdout, "Creating topic: %s", *topicTopic)

	addr, err := net.ResolveTCPAddr("tcp", *topicBrokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error shutting down store: %s\n", err)
		os.Exit(1)
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to broker: %s\n", err)
		os.Exit(1)
	}

	client := server.NewClient(conn)
	resp, err := client.CreateTopic("cmd/createtopic", &protocol.CreateTopicRequest{
		Topic:             *topicTopic,
		NumPartitions:     *topicPartitions,
		ReplicationFactor: *topicReplicationFactor,
		ReplicaAssignment: nil,
		Configs:           nil,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with request to broker: %s\n", err)
		os.Exit(1)
	}
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() {
			err := protocol.Errs[topicErrCode.ErrorCode]
			fmt.Fprintf(os.Stderr, "Error code: %s\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Created topic: %s\n", *topicTopic)

	return 0
}
