package main

import (
	"fmt"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logDirFlag = kingpin.Flag("logdir", "A comma separated list of directories under which to store log files").Default("/tmp/jocko").String()
	httpAddr   = kingpin.Flag("httpaddr", "HTTP Address to listen on").String()
)

func main() {
	kingpin.Parse()

	fmt.Println(*httpAddr)
}
