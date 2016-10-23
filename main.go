package main

import (
	"fmt"
	"os"
	"time"

	gracefully "github.com/tj/go-gracefully"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/jocko/store"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logDir   = kingpin.Flag("logdir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").String()
	httpAddr = kingpin.Flag("httpaddr", "HTTP Address to listen on").String()
	raftDir  = kingpin.Flag("raftdir", "Directory for raft to store data").String()
	raftAddr = kingpin.Flag("raftaddr", "Address for Raft to bind on").String()
)

func main() {
	kingpin.Parse()

	store := store.New(store.Options{
		DataDir:  *raftDir,
		BindAddr: *raftAddr,
		LogDir:   *logDir,
	})
	if err := store.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "Error opening raft store: %s\n", err)
		os.Exit(1)
	}

	server := server.New(*httpAddr, store)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %s\n", err)
		os.Exit(1)
	}

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	store.Close()
	server.Close()
}
