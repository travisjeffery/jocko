package main

import (
	//"fmt"
	//"os"

	"github.com/spf13/cobra"
	//"github.com/travisjeffery/jocko/client"
)

type partitionConfig struct {
	group string
	offset string
}

func partitionCmds(globalCfg *globalConfig) *cobra.Command {
	partCfg := &partitionConfig{}
	partitionCmd := &cobra.Command{Use: "partition", Short: "Manage topics partitions"}
	partitionCmd.Aliases = []string{"part"}

	listPartitionCmd := &cobra.Command{
		Use: "list topicName ...",
		Short: "List topics' partitions (newest,oldest offsets)",
		Run: func(cmd *cobra.Command, args []string) {
			//args are topics names
			listPartitions(globalCfg, args)
		},
	}
	listPartitionCmd.Aliases = []string{"ls"}

	describePartitionCmd := &cobra.Command{
		Use: "describe partitionNames ...",
		Short: "Describe partitions offsets (regarding to group)",
		Run: func(cmd *cobra.Command, args []string) {
			describePartitions(globalCfg,partCfg,args)
		},
	}
	describePartitionCmd.Aliases = []string{"desc"}
	describePartitionCmd.Flags().StringVarP(&partCfg.group, "group", "g", "", "Consumer group to ask about offsets")

	resetPartitionCmd := &cobra.Command{
		Use: "reset partitionNames ...",
		Short: "Reset partitions offsets regarding to group",
		Run: func(cmd *cobra.Command, args []string) {
			resetPartitionsOffset(globalCfg,partCfg,args)
		},
	}
	resetPartitionCmd.Flags().StringVarP(&partCfg.group, "group", "g", "", "Consumer group to reset offsets")
	resetPartitionCmd.Flags().StringVarP(&partCfg.offset, "offset", "x", "", "Offset reset to partition/group")

	partitionCmd.AddCommand(listPartitionCmd)
	partitionCmd.AddCommand(describePartitionCmd)
	partitionCmd.AddCommand(resetPartitionCmd)

	return partitionCmd
}

func listPartitions(globalCfg *globalConfig, topics []string) {
}

func describePartitions(globalCfg *globalConfig, partCfg *partitionConfig, partitionNames []string) {
}

func resetPartitionsOffset(globalCfg *globalConfig, partCfg *partitionConfig, partitionNames []string) {
}
