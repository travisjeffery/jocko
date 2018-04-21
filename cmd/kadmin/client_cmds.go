package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/travisjeffery/jocko/client"
)

func clientCmds() (cmds []*cobra.Command) {
	cmds = append(cmds, topicCmd())
	return
}

var (
	topicCfg = &client.TopicInfo{}
)

func topicCmd() *cobra.Command {
	topicCmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	createTopicCmd := &cobra.Command{Use: "create", Short: "Create a topic", Run: createTopic}
	createTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")
	createTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to create")
	createTopicCmd.Flags().Int32Var(&topicCfg.Partitions, "partitions", 1, "Number of partitions")
	createTopicCmd.Flags().Int32Var(&topicCfg.ReplicationFactor, "replication-factor", 1, "Replication factor")

	deleteTopicCmd := &cobra.Command{Use: "delete", Short: "Delete a topic", Run: deleteTopic}
	deleteTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")
	deleteTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to delete")

	listTopicCmd := &cobra.Command{Use: "list", Short: "List all topics", Run: listTopic}
	listTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")

	describeTopicCmd := &cobra.Command{Use: "describe", Short: "Describe a topic", Run: describeTopic}
	describeTopicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr", "0.0.0.0:9092", "Address for Broker to bind on")
	describeTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to describe")

	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(listTopicCmd)
	topicCmd.AddCommand(describeTopicCmd)

	return topicCmd
}

func createTopic(cmd *cobra.Command, args []string) {
	adm, err := client.NewAdminClient([]string{topicCfg.BrokerAddr}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	err = adm.CreateTopics([]*client.TopicInfo{topicCfg}, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with createTopic request: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("created topic: %v\n", topicCfg.Topic)
}

func deleteTopic(cmd *cobra.Command, args []string) {
	adm, err := client.NewAdminClient([]string{topicCfg.BrokerAddr},nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	err = adm.DeleteTopics([]string{topicCfg.Topic}, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with deleteTopic request: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("deleted topic: %v\n", topicCfg.Topic)
}

func listTopic(cmd *cobra.Command, args []string) {
	adm, err := client.NewAdminClient([]string{topicCfg.BrokerAddr},nil)
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
	for k,_ := range topics {
		fmt.Println(k)
	}
}

func describeTopic(cmd *cobra.Command, args []string) {
	adm, err := client.NewAdminClient([]string{topicCfg.BrokerAddr},nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create AdminClient: %v\n", err)
		os.Exit(1)
	}
	defer adm.Close()

	topinfo, err := adm.DescribeTopics([]string{topicCfg.Topic}, client.DefaultOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with deleteTopic request: %v\n", err)
		os.Exit(1)
	}
	for _, ti := range topinfo {
		fmt.Printf("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:\n",ti.Name,len(ti.Partitions),len(ti.Partitions[0].Replicas))
		for i, p := range ti.Partitions {
			fmt.Printf("\tTopic:%s\tPartition:%d\tLeader:%d\tReplicas:%v\tIsr:%v\n",ti.Name,i,p.Leader,p.Replicas,p.Isr);
		}
	}
	fmt.Printf("described topic: %v\n", topicCfg.Topic)
}
