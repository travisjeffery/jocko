package config

import (
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	DefaultLANSerfPort = 8301
)

// Config holds the configuration for a Config.
type Config struct {
	ID                            int32
	NodeName                      string
	DataDir                       string
	DevMode                       bool
	Addr                          string
	SerfLANConfig                 *serf.Config
	RaftConfig                    *raft.Config
	Bootstrap                     bool
	BootstrapExpect               int
	StartAsLeader                 bool
	StartJoinAddrsLAN             []string
	StartJoinAddrsWAN             []string
	NonVoter                      bool
	RaftAddr                      string
	LeaveDrainTime                time.Duration
	ReconcileInterval             time.Duration
	OffsetsTopicReplicationFactor int16
}

// DefaultConfig creates/returns a default configuration.
func DefaultConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	conf := &Config{
		DevMode:                       false,
		NodeName:                      hostname,
		SerfLANConfig:                 serfDefaultConfig(),
		RaftConfig:                    raft.DefaultConfig(),
		LeaveDrainTime:                5 * time.Second,
		ReconcileInterval:             60 * time.Second,
		OffsetsTopicReplicationFactor: 3,
	}

	conf.SerfLANConfig.ReconnectTimeout = 3 * 24 * time.Hour
	conf.SerfLANConfig.MemberlistConfig.BindPort = DefaultLANSerfPort

	return conf
}

func serfDefaultConfig() *serf.Config {
	base := serf.DefaultConfig()
	base.QueueDepthWarning = 1000000
	return base
}
