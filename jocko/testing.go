package jocko

import (
	"fmt"
	"io/ioutil"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/raft"
	"github.com/mitchellh/go-testing-interface"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/jocko/config"

	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var (
	nodeNumber int32
)

func NewTestServer(t testing.T, cbBroker func(cfg *config.Config), cbServer func(cfg *config.Config)) (*Server, string) {
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

	// jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	tracer, closer, err := cfg.New(
		"jocko",
		// jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		panic(err)
	}

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("jocko-test-server-%d", nodeID))
	if err != nil {
		panic(err)
	}

	config := config.DefaultConfig()
	config.ID = nodeID
	config.NodeName = fmt.Sprintf("%s-node-%d", t.Name(), nodeID)
	config.DataDir = tmpDir
	config.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	config.RaftAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[1])
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.BindPort = ports[2]
	config.LeaveDrainTime = 1 * time.Millisecond
	config.ReconcileInterval = 300 * time.Millisecond

	// Tighten the Serf timing
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfLANConfig.MemberlistConfig.RetransmitMult = 2
	config.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond

	// Tighten the Raft timing
	config.RaftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	config.RaftConfig.HeartbeatTimeout = 200 * time.Millisecond
	config.RaftConfig.ElectionTimeout = 200 * time.Millisecond

	if cbBroker != nil {
		cbBroker(config)
	}

	b, err := NewBroker(config, tracer)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	if cbServer != nil {
		cbServer(config)
	}

	return NewServer(config, b, nil, tracer, closer.Close), tmpDir
}

func TestJoin(t testing.T, s1 *Server, other ...*Server) {
	addr := fmt.Sprintf("127.0.0.1:%d",
		s1.config.SerfLANConfig.MemberlistConfig.BindPort)
	for _, s2 := range other {
		if num, err := s2.handler.(*Broker).serf.Join([]string{addr}, true); err != nil {
			t.Fatalf("err: %v", err)
		} else if num != 1 {
			t.Fatalf("bad: %d", num)
		}
	}
}

// WaitForLeader waits for one of the servers to be leader, failing the test if no one is the leader. Returns the leader (if there is one) and non-leaders.
func WaitForLeader(t testing.T, servers ...*Server) (*Server, []*Server) {
	tmp := struct {
		leader    *Server
		followers map[*Server]bool
	}{nil, make(map[*Server]bool)}
	retry.Run(t, func(r *retry.R) {
		for _, s := range servers {
			if raft.Leader == s.handler.(*Broker).raft.State() {
				tmp.leader = s
			} else {
				tmp.followers[s] = true
			}
		}
		if tmp.leader == nil {
			r.Fatal("no leader")
		}
	})
	followers := make([]*Server, 0, len(tmp.followers))
	for f := range tmp.followers {
		followers = append(followers, f)
	}
	return tmp.leader, followers
}
