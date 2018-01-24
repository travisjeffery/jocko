package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	cli       = kingpin.New("jocko", "JOCKO -- Kafka in Go and more.")
	debugLogs = cli.Flag("debug", "Enable debug logs").Default("false").Bool()
	logger    = log.New()

	brokerCmd       = cli.Command("broker", "Run a Jocko broker")
	brokerCmdConfig = newBrokerOptions(brokerCmd)

	topicCmd          = cli.Command("topic", "Manage topics")
	createTopicCmd    = topicCmd.Command("create", "Create a topic")
	createTopicConfig = newCreateTopicFlags(createTopicCmd)
)

func main() {
	cmd := kingpin.MustParse(cli.Parse(os.Args[1:]))

	switch cmd {
	case brokerCmd.FullCommand():
		os.Exit(cmdBrokers())
	case createTopicCmd.FullCommand():
		os.Exit(cmdCreateTopic())
	}
}

type config struct {
	ID           int32
	DataDir      string
	BrokerConfig *broker.Config
	RaftConfig   *raft.Config
	SerfConfig   *serf.Config
	ServerConfig *server.Config
}

func newBrokerOptions(cmd *kingpin.CmdClause) *config {
	conf := &config{RaftConfig: &raft.Config{}, SerfConfig: &serf.Config{}, ServerConfig: &server.Config{}, BrokerConfig: &broker.Config{}}
	brokerCmd.Flag("raft-addr", "Address for Raft to bind and advertise on").Default("127.0.0.1:9093").StringVar(&conf.RaftConfig.Addr)
	brokerCmd.Flag("data-dir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").StringVar(&conf.DataDir)
	brokerCmd.Flag("broker-addr", "Address for broker to bind on").Default("0.0.0.0:9092").StringVar(&conf.ServerConfig.BrokerAddr)
	brokerCmd.Flag("serf-addr", "Address for Serf to bind on").Default("0.0.0.0:9094").StringVar(&conf.SerfConfig.Addr)
	brokerCmd.Flag("http-addr", "Address for HTTP handlers to serve Prometheus metrics on").Default(":9095").StringVar(&conf.ServerConfig.HTTPAddr)
	brokerCmd.Flag("join", "Address of an broker serf to join at start time. Can be specified multiple times.").StringsVar(&conf.SerfConfig.Join)
	brokerCmd.Flag("join-wan", "Address of an broker serf to join -wan at start time. Can be specified multiple times.").StringsVar(&conf.SerfConfig.JoinWAN)
	brokerCmd.Flag("id", "Broker ID").Int32Var(&conf.ID)
	return conf
}

type createTopicOptions struct {
	Addr              string
	Topic             string
	Partitions        int32
	ReplicationFactor int16
}

func newCreateTopicFlags(cmd *kingpin.CmdClause) *createTopicOptions {
	conf := &createTopicOptions{}
	createTopicCmd.Flag("broker-addr", "Address for Broker to bind on").Default("0.0.0.0:9092").StringVar(&conf.Addr)
	createTopicCmd.Flag("topic", "Name of topic to create").StringVar(&conf.Topic)
	createTopicCmd.Flag("partitions", "Number of partitions").Default("1").Int32Var(&conf.Partitions)
	createTopicCmd.Flag("replication-factor", "Replication factor").Default("1").Int16Var(&conf.ReplicationFactor)
	return conf
}

func cmdBrokers() int {
	var err error
	logger := log.New().With(
		log.Int32("id", brokerCmdConfig.ID),
		log.String("broker addr", brokerCmdConfig.ServerConfig.BrokerAddr),
		log.String("http addr", brokerCmdConfig.ServerConfig.HTTPAddr),
		log.String("serf addr", brokerCmdConfig.SerfConfig.Addr),
		log.String("raft addr", brokerCmdConfig.RaftConfig.Addr),
	)

	serf, err := serf.New(*brokerCmdConfig.SerfConfig, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting serf: %v\n", err)
		os.Exit(1)
	}

	raft, err := raft.New(*brokerCmdConfig.RaftConfig, serf, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting raft: %v\n", err)
		os.Exit(1)
	}

	broker, err := broker.New(*brokerCmdConfig.BrokerConfig, serf, raft, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(*brokerCmdConfig.ServerConfig, broker, nil, logger)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error starting server: %v\n", err)
		os.Exit(1)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := broker.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down store: %v\n", err)
		os.Exit(1)
	}

	return 0
}

func cmdCreateTopic() int {
	addr, err := net.ResolveTCPAddr("tcp", createTopicConfig.Addr)
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
			Topic:             createTopicConfig.Topic,
			NumPartitions:     createTopicConfig.Partitions,
			ReplicationFactor: createTopicConfig.ReplicationFactor,
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

	fmt.Printf("created topic: %v\n", createTopicConfig.Topic)

	return 0
}
