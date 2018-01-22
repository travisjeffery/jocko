package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"
	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/broker/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

var (
	logger = log.New()

	cli = &cobra.Command{
		Use:   "jocko",
		Short: "Kafka in Go and more",
	}

	brokerCfg = struct {
		ID      int32
		DataDir string
		Broker  *config.Config
		Server  *server.Config
	}{
		Broker: config.DefaultConfig(),
		Server: &server.Config{},
	}

	topicCfg = struct {
		BrokerAddr        string
		Topic             string
		Partitions        int32
		ReplicationFactor int
	}{}
)

func init() {
	brokerCmd := &cobra.Command{Use: "broker", Short: "Run a Jocko broker", Run: run}
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.RaftAddr, "raft-addr", "127.0.0.1:9093", "Address for Raft to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.DataDir, "data-dir", "/tmp/jocko", "A comma separated list of directories under which to store log files")
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.Addr, "broker-addr", "0.0.0.0:9092", "Address for broker to bind on")
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.SerfLANConfig.MemberlistConfig.BindAddr, "serf-addr", "0.0.0.0:9094", "Address for Serf to bind on") // TODO: can set addr alone or need to set bind port separately?
	brokerCmd.Flags().StringVar(&brokerCfg.Server.HTTPAddr, "http-addr", ":9095", "Address for HTTP handlers to serve Prometheus metrics on")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.Broker.StartJoinAddrsLAN, "join", nil, "Address of an broker serf to join at start time. Can be specified multiple times.")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.Broker.StartJoinAddrsWAN, "join-wan", nil, "Address of an broker serf to join -wan at start time. Can be specified multiple times.")
	brokerCmd.Flags().Int32Var(&brokerCfg.ID, "id", 0, "Broker ID")

	topicCmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	createTopicCmd := &cobra.Command{Use: "create", Short: "Create a topic", Run: createTopic}
	createTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")
	createTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to create")
	createTopicCmd.Flags().Int32Var(&topicCfg.Partitions, "partitions", 1, "Number of partitions")
	createTopicCmd.Flags().IntVar(&topicCfg.ReplicationFactor, "replication-factor", 1, "Replication factor")

	cli.AddCommand(brokerCmd)
	cli.AddCommand(topicCmd)
	topicCmd.AddCommand(createTopicCmd)
}

func run(cmd *cobra.Command, args []string) {
	var err error
	logger := log.New().With(
		log.Int32("id", brokerCfg.ID),
		log.String("broker addr", brokerCfg.Server.BrokerAddr),
		log.String("http addr", brokerCfg.Server.HTTPAddr),
		log.String("serf addr", brokerCfg.Broker.SerfLANConfig.MemberlistConfig.BindAddr),
		log.String("raft addr", brokerCfg.Broker.RaftAddr),
	)

	broker, err := broker.New(brokerCfg.Broker, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(brokerCfg.Server, broker, nil, logger)
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
}

func createTopic(cmd *cobra.Command, args []string) {
	addr, err := net.ResolveTCPAddr("tcp", topicCfg.BrokerAddr)
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
			Topic:             topicCfg.Topic,
			NumPartitions:     topicCfg.Partitions,
			ReplicationFactor: int16(topicCfg.ReplicationFactor),
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

	fmt.Printf("created topic: %v\n", topicCfg.Topic)
}

func main() {
	cli.Execute()
}
