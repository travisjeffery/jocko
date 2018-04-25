package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/travisjeffery/jocko/client"
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
	cli, err := client.NewClient(globalCfg.brokers,nil,globalCfg.state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create Client: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	groupIds, err := cli.ListGroups(client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with listGroups request: %v\n", err)
		os.Exit(1)
	}
	if globalCfg.printJson {
		printJson(groupIds)
	} else {
		for n, gIds := range groupIds {
			fmt.Println(n,"coordinates groups:")
			for _, id := range gIds {
				fmt.Printf("\t%v\n",id)
			}
		}
	}
}

func describeGroups(globalCfg *globalConfig, args []string) {
	cli, err := client.NewClient(globalCfg.brokers,nil,globalCfg.state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create Client: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	groups, err := cli.DescribeGroups(args, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with describeGroups request: %v\n", err)
		os.Exit(1)
	}
	if globalCfg.printJson {
		printJson(groups)
	} else {
		for _,g := range groups {
			fmt.Println("\nID\tState\tProtocolType\tProtocol")
			fmt.Printf("%s\t%s\t%s\t%s\n",g.GroupID,g.State,g.ProtocolType,g.Protocol)
			if len(g.GroupMembers)>0 {
				fmt.Println("Members\tClientID ClientHost")
				for _,m := range g.GroupMembers {
					fmt.Printf("\t%s\t%s\n",m.ClientID,m.ClientHost)
				}
			}
		}
	}
	return
}
