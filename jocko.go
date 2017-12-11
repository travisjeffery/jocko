package jocko

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

// Client is used to request other brokers.
type Client interface {
	FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error)
	CreateTopics(clientID string, createRequests *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error)
	// others
}

// Alias prometheus' counter, probably only need to use Inc() though.
type Counter = prometheus.Counter

// Metrics is used for tracking metrics.
type Metrics struct {
	RequestsHandled Counter
}

// MemberStatus is the state that a member is in.
type MemberStatus int

// Different possible states of serf member.
const (
	StatusNone MemberStatus = iota
	StatusAlive
	StatusLeaving
	StatusLeft
	StatusFailed
	StatusReap
)

type Config struct {
	ID          int32
	Logger      log.Logger
	BindAddrLAN string
	// Bootstrap is used to bring up the first Consul server, and
	// permits that node to elect itself leader
	Bootstrap bool
	// BootstrapExpect tries to automatically bootstrap the Jocko cluster, by
	// having servers wait to bootstrap until enough servers join, and then
	// performing the bootstrap process automatically. They will disable their
	// automatic bootstrap process if they detect any servers that are part of
	// an existing cluster, so it's safe to leave this set to a non-zero value.
	BoostrapExpect int
	Metrics        *Metrics
	DataDir        string
	Datacenter     string
	// EnableDebug is used to enable various debugging features.
	EnableDebug bool
	// LogLevel is the level of the logs to write. Defaults to "INFO".
	SerfBindAddrLAN   string
	SerfBindAddrWAN   string
	StartJoinAddrsLAN []string
	StartJoinAddrsWAN []string
	SerfPortLAN       int
	SerfPortWAN       int
	Raft              Raft
	Serf              Serf
	Broker            Broker
	RaftBindAddr      string
	HTTPBindAddr      string
	// ServerMode controls if this agent acts like a Jocko server,
	// or merely as a client. Servers have more state, take part
	// in leader election, etc.
	ServerMode                  bool
	LogLevel                    string
	JockoRaftElectionTimeout    time.Duration
	JockoRaftHeartbeatTimeout   time.Duration
	JockoRaftLeaderLeaseTimeout time.Duration
	JockoSerfLANGossipInterval  time.Duration
	JockoSerfLANProbeInterval   time.Duration
	JockoSerfLANProbeTimeout    time.Duration
	JockoSerfLANSuspicionMult   int
	JockoSerfWANGossipInterval  time.Duration
	JockoSerfWANProbeInterval   time.Duration
	JockoSerfWANProbeTimeout    time.Duration
	JockoSerfWANSuspicionMult   int
	JockoServerHealthInterval   time.Duration
}

type RaftCmdType int

type RaftCommand struct {
	Cmd  RaftCmdType      `json:"type"`
	Data *json.RawMessage `json:"data"`
}

// Raft is the interface that wraps Raft's methods and is used to
// manage consensus for the Jocko cluster.
type Raft interface {
	Addr() string
	IsLeader() bool
	LeaderID() string
	Shutdown() error
	Apply(cmd RaftCommand) error
	Bootstrap(serf Serf, serfEventCh <-chan *ClusterMember, commandCh chan<- RaftCommand) error
}

// Serf is the interface that wraps Serf methods and is used to manage
// the cluster membership for Jocko nodes.
type Serf interface {
	Bootstrap(node *ClusterMember, reconcileCh chan<- *ClusterMember) error
	Cluster() []*ClusterMember
	Member(memberID int32) *ClusterMember
	Join(addrs ...string) (int, error)
	Shutdown() error
	ID() int32
}

type RaftEvent struct {
	Op     string
	Member *ClusterMember
}

// Request represents an API request.
type Request struct {
	Conn    io.ReadWriter
	Header  *protocol.RequestHeader
	Request interface{}
}

// Request represents an API request.
type Response struct {
	Conn     io.ReadWriter
	Header   *protocol.RequestHeader
	Response interface{}
}

// Broker is the interface that wraps the Broker's methods.
type Broker interface {
	ID() int32
	Run(context.Context, <-chan Request, chan<- Response)
	Join(addr ...string) protocol.Error
	Shutdown() error
}

// ClusterMember is used as a wrapper around a broker's info and a
// connection to it.
type ClusterMember struct {
	ID   int32  `json:"id"`
	Port int    `json:"port"`
	IP   string `json:"addr"`

	SerfPort int          `json:"-"`
	RaftPort int          `json:"-"`
	Status   MemberStatus `json:"-"`

	conn net.Conn
}

// Addr is used to get the address of the member.
func (b *ClusterMember) Addr() *net.TCPAddr {
	return &net.TCPAddr{IP: net.ParseIP(b.IP), Port: b.Port}
}

// Write is used to write the member.
func (b *ClusterMember) Write(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Write(p)
}

// Read is used to read from the member.
func (b *ClusterMember) Read(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Read(p)
}

// connect opens a tcp connection to the cluster member.
func (b *ClusterMember) connect() error {
	addr := &net.TCPAddr{IP: net.ParseIP(b.IP), Port: b.Port}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}

//go:generate mocker --prefix "" --out mock/broker.go --pkg mock . Broker
//go:generate mocker --prefix "" --out mock/commitlog.go --pkg mock . CommitLog
//go:generate mocker --prefix "" --out mock/serf.go --pkg mock . Serf
//go:generate mocker --prefix "" --out mock/raft.go --pkg mock . Raft
