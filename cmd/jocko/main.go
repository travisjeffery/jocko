package main

import (
	"fmt"
	"os"
	"net"
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
	cli       = kingpin.New("jocko", "Jocko, a Go implementation of Kafka")
	logDir    = cli.Flag("logdir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").String()
	debugLogs = cli.Flag("debug", "Enable debug logs").Default("false").Bool()

	brokerCmd            = cli.Command("broker", "Operations on brokers")
	brokerCmdRaftAddr    = brokerCmd.Flag("raftaddr", "Address for Raft to bind and advertise on").Default("127.0.0.1:9093").String()
	brokerCmdBrokerAddr  = brokerCmd.Flag("brokeraddr", "Address for broker to bind on").Default("0.0.0.0:9092").String()
	brokerCmdSerfAddr    = brokerCmd.Flag("serfaddr", "Address for Serf to bind on").Default("0.0.0.0:9094").String()
	brokerCmdSerfMembers = brokerCmd.Flag("serfmembers", "List of existing Serf members").Strings()
	brokerCmdBrokerID    = brokerCmd.Flag("id", "Broker ID").Int32()

	topic                  = cli.Command("topic", "Operations on topics")
	topicBrokerAddr        = topic.Flag("brokeraddr", "Address for Broker to bind on").Default("0.0.0.0:9092").String()
	topicTopic             = topic.Flag("topic", "Name of topic to create").String()
	topicPartitions        = topic.Flag("partitions", "Number of partitions").Default("1").Int32()
	topicReplicationFactor = topic.Flag("replicationfactor", "Replication factor").Default("1").Int16()
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

	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(*logDir),
		raft.Addr(*brokerCmdRaftAddr),
	)

	store, err := broker.New(*brokerCmdBrokerID,
		broker.LogDir(*logDir),
		broker.Logger(logger),
		broker.Addr(*brokerCmdBrokerAddr),
		broker.Serf(serf),
		broker.Raft(raft),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with new broker: %s\n", err)
		os.Exit(1)
	}
	srv := server.New(*brokerCmdBrokerAddr, store, logger)
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := store.Shutdown(); err != nil {
		panic(err)
	}

	return 0
}

func CmdTopic(logger *simplelog.Logger) int {
	logger.Info("Create topic")
	addr, err := net.ResolveTCPAddr("tcp", *topicBrokerAddr)
	if err != nil {
		panic(err)
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		panic(err)
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
		panic(err)
	}

	for _, topicErrCode := range resp.TopicErrorCodes {
		msg := "ok"
		if topicErrCode.ErrorCode == 41 {
			msg = "err not controller"
		}
		fmt.Printf("create topic %s: %s\n", topicErrCode.Topic, msg)
	}

	return 0
}
