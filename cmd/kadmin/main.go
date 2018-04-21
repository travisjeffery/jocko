package main

import (
	"github.com/spf13/cobra"
)

var (
	cli = &cobra.Command{
		Use:   "jocko",
		Short: "Kafka client in Go and more",
	}
)

func init() {
	//add client commands
	for _, ccmd := range clientCmds() {
		cli.AddCommand(ccmd)
	}
}

func main() {
	cli.Execute()
}
