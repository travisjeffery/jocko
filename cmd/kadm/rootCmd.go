package main

import (
	//"fmt"
	//"os"

	"github.com/spf13/cobra"
)

type globalConfig struct {
	brokers []string
}

func rootCmd_Execute() {
	gCfg := &globalConfig{}
	rootCmd := &cobra.Command{
		Use:   "kadm",
		Short: "Kafka admin client in Go",
		Long: "Use kadm to do basic management of Kafka cluster",
	}
	rootCmd.PersistentFlags().StringSliceVarP(&gCfg.brokers,"brokers","b",nil,"Bootstrap brokers to connect")

	rootCmd.AddCommand(nodeCmds(gCfg))
	rootCmd.AddCommand(topicCmds(gCfg))

	rootCmd.Execute()
}
