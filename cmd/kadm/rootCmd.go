package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/travisjeffery/jocko/client"
)

const (
	Kadm_File=".kadm_state"
	Kadm_File_Path="KADM_STATE"
)

type globalConfig struct {
	brokers []string
	printJson bool //default "tabular",  can change to json
	state *client.ClusterState
}

func rootCmd_Execute() {
	gCfg := &globalConfig{state:&client.ClusterState{}}
	rootCmd := &cobra.Command{
		Use:   "kadm",
		Short: "Kafka admin client in Go",
		Long: "Use kadm to do basic management of Kafka cluster",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			err := loadClusterState(gCfg)
			if err != nil {
				fmt.Println("no kadm state loaded:",err)
			}
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			err:= saveClusterState(gCfg)
			if err != nil {
				fmt.Println("fail to save state:",err)
			}
		},
	}
	rootCmd.PersistentFlags().StringSliceVarP(&gCfg.brokers,"brokers","b",nil,"Bootstrap brokers to connect")
	rootCmd.PersistentFlags().BoolVar(&gCfg.printJson,"json",false,"Print results in json for possible script consumption")

	rootCmd.AddCommand(initCmd(gCfg))
	rootCmd.AddCommand(nodeCmds(gCfg))
	rootCmd.AddCommand(topicCmds(gCfg))
	rootCmd.AddCommand(groupCmds(gCfg))
	rootCmd.AddCommand(partitionCmds(gCfg))

	rootCmd.Execute()
}

func initCmd(gCfg *globalConfig) *cobra.Command {
	return &cobra.Command {
		Use: "init --brokers(-b) brokerAddr1,...",
		Short: "Init(reset) kadm state for next session",
		Long: `Init(reset) kadm state for next session; kadm state is saved in file ./.kadm_state or file named by environment variable $KADM_STATE`,
		Run: func(cmd *cobra.Command, args []string) {
			describeCluster(gCfg)
		},
	}
}

func kadmFname() (fname string, err error) {
	fname = os.Getenv(Kadm_File_Path)
	if fname != "" { return }
	wd,err := os.Getwd()
	if err!=nil { return }
	fname = filepath.Join(wd,Kadm_File)
	return
}

func loadClusterState(gCfg *globalConfig) (err error) {
	fname, err := kadmFname()
	if err != nil { return }
	buf, err := ioutil.ReadFile(fname)
	if err != nil { return }
	return json.Unmarshal(buf,gCfg.state)
}

func saveClusterState(gCfg *globalConfig) (err error) {
	fname, err := kadmFname()
	if err != nil { return }
	buf, err := json.MarshalIndent(gCfg.state,"","  ")
	if err != nil { return }
	//fmt.Printf("save cluster state to %s:\n%s\n",fname,string(buf))
	return ioutil.WriteFile(fname, buf, 0600)
}

func printJson(data interface{}) error {
	buf, err := json.MarshalIndent(data,"","  ")
	if err == nil {
		fmt.Println(string(buf))
	}
	return err
}
