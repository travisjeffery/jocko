package main

import (
	"fmt"
	"os"
	"time"

	"github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
	"gopkg.in/alecthomas/kingpin.v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	logDir      = kingpin.Flag("logdir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").String()
	raftAddr    = kingpin.Flag("raftaddr", "Address for Raft to bind and advertise on").Default("127.0.0.1:9093").String()
	brokerAddr  = kingpin.Flag("brokeraddr", "Address for Broker to bind on").Default("0.0.0.0:9092").String()
	serfAddr    = kingpin.Flag("serfaddr", "Address for Serf to bind on").Default("0.0.0.0:9094").String()
	promMetrics = kingpin.Flag("prommetrics", "Enable Prometheus metrics").Default("true").Bool()
	promAddr    = kingpin.Flag("promaddr", "Address for Prometheus to serve metrics").Default(":9095").String()
	serfMembers = kingpin.Flag("serfmembers", "List of existing Serf members").Strings()
	brokerID    = kingpin.Flag("id", "Broker ID").Int32()
	debugLogs   = kingpin.Flag("debug", "Enable debug logs").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	logLevel := simplelog.INFO
	if *debugLogs {
		logLevel = simplelog.DEBUG
	}
	logger := simplelog.New(os.Stdout, logLevel, "jocko")

	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr(*serfAddr),
		serf.InitMembers(*serfMembers),
	)

	raft, err := raft.New(
		raft.Logger(logger),
		raft.DataDir(*logDir),
		raft.Addr(*raftAddr),
	)

	store, err := broker.New(*brokerID,
		broker.LogDir(*logDir),
		broker.Logger(logger),
		broker.Addr(*brokerAddr),
		broker.Serf(serf),
		broker.Raft(raft),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with new broker: %s\n", err)
		os.Exit(1)
	}

	var r prometheus.Registerer = nil
	if *promMetrics {
		r = prometheus.DefaultRegisterer
	}
	srv := server.New(*brokerAddr, store, logger, r)
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	if r != nil {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(*promAddr, nil)
	}

	defer srv.Close()

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := store.Shutdown(); err != nil {
		panic(err)
	}
}
