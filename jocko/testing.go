package jocko

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/mitchellh/go-testing-interface"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
)

var (
	nodeNumber int32
	tempDir    string
	logger     = log.New()
)

func init() {
	var err error
	tempDir, err = ioutil.TempDir("", "jocko-test-cluster")
	if err != nil {
		panic(err)
	}
}

func NewTestServer(t testing.T, cbBroker func(cfg *config.BrokerConfig), cbServer func(cfg *ServerConfig)) *Server {
	ports := dynaport.GetS(4)
	nodeID := atomic.AddInt32(&nodeNumber, 1)

	brokerConfig := config.DefaultBrokerConfig()
	brokerConfig.DataDir = filepath.Join(tempDir, fmt.Sprintf("node%d", nodeID))
	brokerConfig.Addr = "127.0.0.1:" + ports[0]
	brokerConfig.RaftAddr = "127.0.0.1:" + ports[1]
	brokerConfig.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1:" + ports[2]

	// Tighten the Serf timing
	brokerConfig.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	brokerConfig.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	brokerConfig.SerfLANConfig.MemberlistConfig.RetransmitMult = 2
	brokerConfig.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	brokerConfig.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	brokerConfig.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond

	// Tighten the Raft timing
	brokerConfig.RaftConfig.LeaderLeaseTimeout = 50 * time.Millisecond
	brokerConfig.RaftConfig.HeartbeatTimeout = 50 * time.Millisecond
	brokerConfig.RaftConfig.ElectionTimeout = 50 * time.Millisecond

	if cbBroker != nil {
		cbBroker(brokerConfig)
	}

	b, err := NewBroker(brokerConfig, logger)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	serverConfig := &ServerConfig{
		BrokerAddr: brokerConfig.Addr,
		HTTPAddr:   "127.0.0.1:" + ports[3],
	}

	if cbServer != nil {
		cbServer(serverConfig)
	}

	return NewServer(serverConfig, b, nil, logger)
}

func TestJoin(t testing.T, s1 *Server, other ...*Server) {
	addr := fmt.Sprintf("127.0.0.1:%d",
		s1.broker.config.SerfLANConfig.MemberlistConfig.BindPort)
	for _, s2 := range other {
		if num, err := s2.broker.serf.Join([]string{addr}, true); err != nil {
			t.Fatalf("err: %v", err)
		} else if num != 1 {
			t.Fatalf("bad: %d", num)
		}
	}
}
