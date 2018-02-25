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

	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
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
	ports := dynaport.Get(4)
	nodeID := atomic.AddInt32(&nodeNumber, 1)

	cfg := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	tracer, closer, err := cfg.New(
		"jocko",
		jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		panic(err)
	}

	brokerConfig := config.DefaultBrokerConfig()
	brokerConfig.ID = nodeID
	brokerConfig.NodeName = fmt.Sprintf("%s-node-%d", t.Name(), nodeID)
	brokerConfig.DataDir = filepath.Join(tempDir, fmt.Sprintf("node%d", nodeID))
	brokerConfig.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	brokerConfig.RaftAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[1])
	brokerConfig.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	brokerConfig.SerfLANConfig.MemberlistConfig.BindPort = ports[2]

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

	b, err := NewBroker(brokerConfig, tracer, logger)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	serverConfig := &ServerConfig{
		BrokerAddr: brokerConfig.Addr,
	}

	if cbServer != nil {
		cbServer(serverConfig)
	}

	return NewServer(serverConfig, b, nil, tracer, closer.Close, logger)
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
