package main

import (
	//"fmt"
	//"os"

	"github.com/spf13/cobra"
	//"github.com/travisjeffery/jocko/client"
)

func groupCmds(globalCfg *globalConfig) *cobra.Command {
	//groupCfg := &client.GroupInfo{}
	groupCmd := &cobra.Command{Use: "group", Short: "Manage consumer groups"}
	groupCmd.Aliases = []string{"grp"}

	listGroupCmd := &cobra.Command{
		Use: "list",
		Short: "List all groups",
		Run: func(cmd *cobra.Command, args []string) {
			listGroups(globalCfg)
		},
	}
	listGroupCmd.Aliases = []string{"ls"}
	
	describeGroupCmd := &cobra.Command{
		Use: "describe groupNames ...",
		Short: "Describe groups",
		Run: func(cmd *cobra.Command, args []string) {
			describeGroups(globalCfg,args)
		},
	}
	describeGroupCmd.Aliases = []string{"desc"}
	
	groupCmd.AddCommand(listGroupCmd)
	groupCmd.AddCommand(describeGroupCmd)

	return groupCmd
}

func listGroups(globalCfg *globalConfig) {
}

func describeGroups(globalCfg *globalConfig, args []string) {
}
