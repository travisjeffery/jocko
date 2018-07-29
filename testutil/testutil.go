package testutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
)

func TestConfig(t *testing.T) (string, *config.Config) {
	dir := tempDir(t, "jocko")
	config := config.DefaultConfig()
	ports := dynaport.Get(3)
	config.NodeName = uniqueNodeName(t.Name())
	config.Bootstrap = true
	config.DataDir = dir
	config.ID = atomic.AddInt32(&nodeID, 1)
	config.SerfLANConfig.Logger = log.NewStdLogger(log.New(log.DebugLevel, fmt.Sprintf("serf/%d: ", config.ID)))
	config.OffsetsTopicReplicationFactor = 3
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.BindPort = ports[1]
	config.SerfLANConfig.MemberlistConfig.AdvertiseAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.AdvertisePort = ports[1]
	config.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.Logger = log.NewStdLogger(log.New(log.DebugLevel, fmt.Sprintf("memberlist/%d: ", config.ID)))
	config.RaftConfig.Logger = log.NewStdLogger(log.New(log.DebugLevel, fmt.Sprintf("raft/%d: ", config.ID)))
	config.RaftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	config.RaftConfig.HeartbeatTimeout = 200 * time.Millisecond
	config.RaftConfig.ElectionTimeout = 200 * time.Millisecond
	config.RaftAddr = fmt.Sprintf("127.0.0.1:%d", ports[2])
	config.Addr = "localhost:9092"
	return dir, config
}

var tmpDir = "/tmp/jocko-test"

func init() {
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		fmt.Printf("Cannot create %s. Reverting to /tmp\n", tmpDir)
		tmpDir = "/tmp"
	}
}

func tempDir(t *testing.T, name string) string {
	if t != nil && t.Name() != "" {
		name = t.Name() + "-" + name
	}
	name = strings.Replace(name, "/", "_", -1)
	d, err := ioutil.TempDir(tmpDir, name)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return d
}

var nodeID int32

var id int64

func uniqueNodeName(name string) string {
	return fmt.Sprintf("%s-node-%d", name, atomic.AddInt64(&id, 1))
}
