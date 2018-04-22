package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/travisjeffery/jocko/client"
)

func nodeCmds(globalCfg *globalConfig) *cobra.Command {
	nodeCmd := &cobra.Command{
		Use: "node",
		Short: "Information about nodes in cluster",
	}
	apiVersionCmd := &cobra.Command{
		Use: "apiversion [hostName ...]",
		Short: "Retrieve nodes api version",
		Run: func(cmd *cobra.Command, args []string) {
			apiVersions(args)
		},
	}
	apiVersionCmd.Aliases = []string{"apiv"}
	listCmd := &cobra.Command{
		Use: "list",
		Short: "List all nodes in cluster",
		Run: func(cmd *cobra.Command, args []string) {
			describeCluster(globalCfg)
		},
	}
	listCmd.Aliases = []string{"ls"}
	describeCmd := &cobra.Command{
		Use: "describe hostNames ...",
		Short: "Describe nodes topic/partition info",
		Run: func(cmd *cobra.Command, args []string) {
			describeNodes(globalCfg,args)
		},
	}
	describeCmd.Aliases = []string{"desc"}
	
	nodeCmd.AddCommand(apiVersionCmd)
	nodeCmd.AddCommand(listCmd)
	nodeCmd.AddCommand(describeCmd)

	return nodeCmd
}

func apiVersions(args []string) {
	adm, err := client.NewAdminClient(nil,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()
	hosts := args
	versionInfo, err := adm.APIVersions(hosts,nil)
	for hname, versions := range versionInfo {
		fmt.Println(hname, "supports [APIKey, MinVersion, MaxVersion]: ")
		for _, v := range versions {
			fmt.Printf("\t%d\t%d\t%d\n",v.APIKey, v.MinVersion, v.MaxVersion)
		}
	}
}

func describeCluster(globalCfg *globalConfig) {
	adm, err := client.NewAdminClient(globalCfg.brokers,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	cinfo, err := adm.DescribeCluster(client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with describeCluster request: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Cluster controller:\n\t%v\n",cinfo.Controller)
	fmt.Println("Nodes:")
	for _,n := range cinfo.Nodes {
		fmt.Printf("\t%v\n",n)
	}
}

func describeNodes(globalCfg *globalConfig, args []string) {
	cli, err := client.NewClient(globalCfg.brokers,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create Client: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	info, err := cli.DescribeNodes(args,client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with describeNodes request: %v\n", err)
		os.Exit(1)
	}
	for nname,nodeTopInfo := range info {
		fmt.Println(nname,"topic partition info:")
		for _,nti := range nodeTopInfo {
			fmt.Printf("\tTopic:%s\tPartition:%d\tInternal:%v\tLeader:%v\tReplicas:%v\tIsr:%v\n",nti.TopicName,nti.PartitionID,nti.IsInternal,nti.IsLeader,nti.Replicas,nti.Isr)
		}
		fmt.Println()
	}
}
