package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/hashicorp/memberlist"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
)

var (
	cli = &cobra.Command{
		Use:   "jocko",
		Short: "Kafka in Go and more",
	}

	brokerCfg = config.DefaultConfig()

	topicCfg = struct {
		BrokerAddr        string
		Topic             string
		Partitions        int32
		ReplicationFactor int
	}{}
)

func init() {
	brokerCmd := &cobra.Command{Use: "broker", Short: "Run a Jocko broker", Run: run, Args: cobra.NoArgs}
	brokerCmd.Flags().StringVar(&brokerCfg.RaftAddr, "raft-addr", "127.0.0.1:9093", "Address for Raft to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.DataDir, "data-dir", "/tmp/jocko", "A comma separated list of directories under which to store log files")
	brokerCmd.Flags().StringVar(&brokerCfg.Addr, "broker-addr", "0.0.0.0:9092", "Address for broker to bind on")
	brokerCmd.Flags().Var(newMemberlistConfigValue(brokerCfg.SerfLANConfig.MemberlistConfig, "0.0.0.0:9094"), "serf-addr", "Address for Serf to bind on")
	brokerCmd.Flags().BoolVar(&brokerCfg.Bootstrap, "bootstrap", false, "Initial cluster bootstrap (dangerous!)")
	brokerCmd.Flags().IntVar(&brokerCfg.BootstrapExpect, "bootstrap-expect", 0, "Expected number of nodes in cluster")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.StartJoinAddrsLAN, "join", nil, "Address of an broker serf to join at start time. Can be specified multiple times.")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.StartJoinAddrsWAN, "join-wan", nil, "Address of an broker serf to join -wan at start time. Can be specified multiple times.")
	brokerCmd.Flags().Int32Var(&brokerCfg.ID, "id", 0, "Broker ID")

	topicCmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	createTopicCmd := &cobra.Command{Use: "create", Short: "Create a topic", Run: createTopic, Args: cobra.NoArgs}
	createTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")
	createTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to create (required)")
	createTopicCmd.MarkFlagRequired("topic")
	createTopicCmd.Flags().Int32Var(&topicCfg.Partitions, "partitions", 1, "Number of partitions")
	createTopicCmd.Flags().IntVar(&topicCfg.ReplicationFactor, "replication-factor", 1, "Replication factor")

	cli.AddCommand(brokerCmd)
	cli.AddCommand(topicCmd)
	topicCmd.AddCommand(createTopicCmd)
}

func run(cmd *cobra.Command, args []string) {
	var err error

	log.SetPrefix(fmt.Sprintf("jocko: node id: %d: ", brokerCfg.ID))

	cfg := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	tracer, closer, err := cfg.New(
		"jocko",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		panic(err)
	}

	broker, err := jocko.NewBroker(brokerCfg, tracer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := jocko.NewServer(brokerCfg, broker, nil, tracer, closer.Close)
	if err := srv.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error starting server: %v\n", err)
		os.Exit(1)
	}

	defer srv.Shutdown()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := broker.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down store: %v\n", err)
		os.Exit(1)
	}
}

func createTopic(cmd *cobra.Command, args []string) {
	conn, err := jocko.Dial("tcp", topicCfg.BrokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error connecting to broker: %v\n", err)
		os.Exit(1)
	}

	resp, err := conn.CreateTopics(&protocol.CreateTopicRequests{
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

type memberlistConfigValue memberlist.Config

func newMemberlistConfigValue(p *memberlist.Config, val string) (m *memberlistConfigValue) {
	m = (*memberlistConfigValue)(p)
	m.Set(val)
	return
}

func (v *memberlistConfigValue) Set(s string) error {
	bindIP, bindPort, err := net.SplitHostPort(s)
	if err != nil {
		return err
	}
	v.BindAddr = bindIP
	v.BindPort, err = strconv.Atoi(bindPort)
	if err != nil {
		return err
	}
	return nil
}

func (v *memberlistConfigValue) Type() string {
	return "string"
}

func (v *memberlistConfigValue) String() string {
	return fmt.Sprintf("%s:%d", v.BindAddr, v.BindPort)
}
