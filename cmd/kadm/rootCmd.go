package main

import (
	"fmt"
	"encoding/json"

	"github.com/spf13/cobra"
)

type globalConfig struct {
	brokers []string
	printJson bool //default "tabular",  can change to json
}

func rootCmd_Execute() {
	gCfg := &globalConfig{}
	rootCmd := &cobra.Command{
		Use:   "kadm",
		Short: "Kafka admin client in Go",
		Long: "Use kadm to do basic management of Kafka cluster",
	}
	rootCmd.PersistentFlags().StringSliceVarP(&gCfg.brokers,"brokers","b",nil,"Bootstrap brokers to connect")
	rootCmd.PersistentFlags().BoolVar(&gCfg.printJson,"json",false,"Print results in json for possible script consumption")

	rootCmd.AddCommand(nodeCmds(gCfg))
	rootCmd.AddCommand(topicCmds(gCfg))
	rootCmd.AddCommand(groupCmds(gCfg))
	rootCmd.AddCommand(partitionCmds(gCfg))

	rootCmd.Execute()
}

func printJson(data interface{}) error {
	marshal := func(v interface{}) ([]byte, error) { return json.MarshalIndent(v, "", "  ") }
	buf, err := marshal(data)
	if err == nil {
		fmt.Println(string(buf))
	}
	return err
}
