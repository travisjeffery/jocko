package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	logDir      = kingpin.Flag("logdir", "A directory under which to store log files").Default("/tmp/jocko").String()
	tcpAddr     = kingpin.Flag("tcpaddr", "Address for HTTP to bind").Default(":8080").String()
	raftAddr    = kingpin.Flag("raftaddr", "Address for Raft to bind").Default(":9020").String()
	serfAddr    = kingpin.Flag("serfaddr", "Address for Serf to bind").Default(":7946").String()
	serfMembers = kingpin.Flag("serfmembers", "Comma-separated list of existing Serf members").Strings()
	brokerID    = kingpin.Flag("id", "Broker ID").Default("1").Int32()
	debugLogs   = kingpin.Flag("debug", "Enable debug logs").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	logLevel := simplelog.INFO
	if *debugLogs {
		logLevel = simplelog.DEBUG
	}
	logger := simplelog.New(os.Stdout, logLevel, "jocko")

	addr, err := net.ResolveTCPAddr("tcp", *tcpAddr)
	if err != nil {
		panic(err)
	}

	store, err := broker.New(*brokerID,
		broker.DataDir(*logDir),
		broker.LogDir(*logDir),
		broker.Logger(logger),
		broker.BindAddr(*raftAddr),
		broker.SerfAddr(*serfAddr),
		broker.SerfMembers(*serfMembers),
		broker.Port(addr.Port),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with new broker: %s\n", err)
		os.Exit(1)
	}
	srv := server.New(*tcpAddr, store, logger)
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
}
