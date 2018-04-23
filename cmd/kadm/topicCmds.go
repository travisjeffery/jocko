package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/travisjeffery/jocko/client"
)


func topicCmds(globalCfg *globalConfig) *cobra.Command {
	topicCfg := &client.TopicInfo{}
	topicCmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	topicCmd.Aliases = []string{"top"}
	
	createTopicCmd := &cobra.Command{
		Use: "create [OPTIONS] topicNames ...",
		Short: "Create topics",
		Run: func(cmd *cobra.Command, args []string) {
			createTopic(globalCfg,topicCfg,args)
		},
	}
	createTopicCmd.Aliases = []string{"add"}
	createTopicCmd.Flags().Int32VarP(&topicCfg.Partitions, "partitions", "p", 1, "Number of partitions")
	createTopicCmd.Flags().Int32VarP(&topicCfg.ReplicationFactor, "replication-factor", "r", 1, "Replication factor")

	deleteTopicCmd := &cobra.Command{
		Use: "delete topicNames ...",
		Short: "Delete topics",
		Run: func(cmd *cobra.Command, args []string) {
			deleteTopic(globalCfg,topicCfg,args)
		},
	}
	deleteTopicCmd.Aliases = []string{"rm"}

	listTopicCmd := &cobra.Command{
		Use: "list",
		Short: "List all topics",
		Run: func(cmd *cobra.Command, args []string) {
			listTopic(globalCfg,topicCfg,args)
		},
	}
	listTopicCmd.Aliases = []string{"ls"}
	
	describeTopicCmd := &cobra.Command{
		Use: "describe topicNames ...",
		Short: "Describe topics",
		Run: func(cmd *cobra.Command, args []string) {
			describeTopic(globalCfg,topicCfg,args)
		},
	}
	describeTopicCmd.Aliases = []string{"desc"}
	
	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(listTopicCmd)
	topicCmd.AddCommand(describeTopicCmd)

	return topicCmd
}

func createTopic(globalCfg *globalConfig, topicCfg *client.TopicInfo, args []string) {
	adm, err := client.NewAdminClient(globalCfg.brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	var topics []*client.TopicInfo
	for _, tn := range args {
		topics = append(topics,&client.TopicInfo{tn,topicCfg.Partitions,topicCfg.ReplicationFactor})
	}
	err = adm.CreateTopics(topics, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with createTopic request: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("created topics: %v\n", args)

}

func deleteTopic(globalCfg *globalConfig, topicCfg *client.TopicInfo, args []string) {
	adm, err := client.NewAdminClient(globalCfg.brokers,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	err = adm.DeleteTopics(args, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with deleteTopic request: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("deleted topics: %v\n", args)
}

func listTopic(globalCfg *globalConfig, topicCfg *client.TopicInfo, args []string) {
	adm, err := client.NewAdminClient(globalCfg.brokers,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	topics, err := adm.ListTopics(client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with listTopic request: %v\n", err)
		os.Exit(1)
	}
	if globalCfg.printJson {
		printJson(topics)
	} else {
		for k,_ := range topics {
			fmt.Println(k)
		}
	}
}

func describeTopic(globalCfg *globalConfig, topicCfg *client.TopicInfo, args []string) {
	adm, err := client.NewAdminClient(globalCfg.brokers,nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	topinfo, err := adm.DescribeTopics(args, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with deleteTopic request: %v\n", err)
		os.Exit(1)
	}
	if globalCfg.printJson {
		printJson(topinfo)
	} else {
		for _, ti := range topinfo {
			fmt.Printf("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:\n",ti.Name,len(ti.Partitions),len(ti.Partitions[0].Replicas))
			for i, p := range ti.Partitions {
				fmt.Printf("\tTopic:%s\tPartition:%d\tLeader:%d\tReplicas:%v\tIsr:%v\n",ti.Name,i,p.Leader,p.Replicas,p.Isr);
			}
		}
		fmt.Printf("described topics: %v\n", args)
	}
}
