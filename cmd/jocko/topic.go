package main

import (
	"github.com/mitchellh/cli"
)

type TopicCommand struct {
}

func (c *TopicCommand) Help() string {
	return "This command is accessed by using one of the subcomands below."
}

func (c *TopicCommand) Synopsis() string {
	return "Manage topics."
}

func (c *TopicCommand) Run(args []string) int {
	return cli.RunResultHelp
}
