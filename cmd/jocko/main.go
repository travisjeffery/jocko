package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko"
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

func newBrokerOptions(cmd *kingpin.CmdClause) jocko.Config {
	conf := jocko.Config{}
	brokerCmd.Flag("raft-addr", "Address for Raft to bind and advertise on").Default("127.0.0.1:9093").StringVar(&conf.RaftBindAddr)
	brokerCmd.Flag("data-dir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").StringVar(&conf.DataDir)
	brokerCmd.Flag("broker-addr", "Address for broker to bind on").Default("0.0.0.0:9092").StringVar(&conf.BindAddrLAN)
	brokerCmd.Flag("serf-addr", "Address for Serf to bind on").Default("0.0.0.0:9094").StringVar(&conf.SerfBindAddrLAN)
	brokerCmd.Flag("http-addr", "Address for HTTP handlers to serve Prometheus metrics on").Default(":9095").StringVar(&conf.HTTPBindAddr)
	brokerCmd.Flag("join", "Address of an broker serf to join at start time. Can be specified multiple times.").StringsVar(&conf.StartJoinAddrsLAN)
	brokerCmd.Flag("join-wan", "Address of an broker serf to join -wan at start time. Can be specified multiple times.").StringsVar(&conf.StartJoinAddrsWAN)
	brokerCmd.Flag("id", "Broker ID").Int32Var(&conf.ID)
	return conf
}

type createTopicOptions struct {
	Addr              string
	Topic             string
	Partitions        int32
	ReplicationFactor int16
}

func newCreateTopicFlags(cmd *kingpin.CmdClause) createTopicOptions {
	conf := createTopicOptions{}
	createTopicCmd.Flag("broker-addr", "Address for Broker to bind on").Default("0.0.0.0:9092").StringVar(&conf.Addr)
	createTopicCmd.Flag("topic", "Name of topic to create").StringVar(&conf.Topic)
	createTopicCmd.Flag("partitions", "Number of partitions").Default("1").Int32Var(&conf.Partitions)
	createTopicCmd.Flag("replication-factor", "Replication factor").Default("1").Int16Var(&conf.ReplicationFactor)
	return conf
}

func cmdBrokers() int {
	var err error

	brokerCmdConfig.Serf, err = serf.New(brokerCmdConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting serf: %v\n", err)
		os.Exit(1)
	}

	brokerCmdConfig.Raft, err = raft.New(brokerCmdConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting raft: %v\n", err)
		os.Exit(1)
	}

	brokerCmdConfig.Broker, err = broker.New(brokerCmdConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(brokerCmdConfig)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error starting server: %v\n", err)
		os.Exit(1)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := brokerCmdConfig.Broker.Shutdown(); err != nil {
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
