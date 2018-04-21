package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
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
		Broker  *config.BrokerConfig
		Server  *config.ServerConfig
	}{
		Broker: config.DefaultBrokerConfig(),
		Server: &config.ServerConfig{},
	}
)

func init() {
	brokerCmd := &cobra.Command{Use: "broker", Short: "Run a Jocko broker", Run: run}
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.RaftAddr, "raft-addr", "127.0.0.1:9093", "Address for Raft to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.DataDir, "data-dir", "/tmp/jocko", "A comma separated list of directories under which to store log files")
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.Addr, "broker-addr", "0.0.0.0:9092", "Address for broker to bind on")
	brokerCmd.Flags().StringVar(&brokerCfg.Broker.SerfLANConfig.MemberlistConfig.BindAddr, "serf-addr", "0.0.0.0:9094", "Address for Serf to bind on") // TODO: can set addr alone or need to set bind port separately?
	brokerCmd.Flags().StringSliceVar(&brokerCfg.Broker.StartJoinAddrsLAN, "join", nil, "Address of an broker serf to join at start time. Can be specified multiple times.")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.Broker.StartJoinAddrsWAN, "join-wan", nil, "Address of an broker serf to join -wan at start time. Can be specified multiple times.")
	brokerCmd.Flags().Int32Var(&brokerCfg.ID, "id", 0, "Broker ID")

	cli.AddCommand(brokerCmd)

	//add client commands
	for _, ccmd := range clientCmds() {
		cli.AddCommand(ccmd)
	}
}

func run(cmd *cobra.Command, args []string) {
	var err error
	logger := log.New().With(
		log.Int32("id", brokerCfg.ID),
		log.String("broker addr", brokerCfg.Server.BrokerAddr),
		log.String("serf addr", brokerCfg.Broker.SerfLANConfig.MemberlistConfig.BindAddr),
		log.String("raft addr", brokerCfg.Broker.RaftAddr),
	)

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

	broker, err := jocko.NewBroker(brokerCfg.Broker, tracer, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	srv := jocko.NewServer(brokerCfg.Server, broker, nil, tracer, closer.Close, logger)
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

func main() {
	cli.Execute()
}
