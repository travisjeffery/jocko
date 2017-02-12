package jocko

import (
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/protocol"
)

type CommitLog interface {
	Init() error
	Open() error
	DeleteAll() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	TruncateTo(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
}

type Partition struct {
	Topic           string  `json:"topic"`
	ID              int32   `json:"id"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	Leader          int32   `json:"leader"`
	PreferredLeader int32   `json:"preferred_leader"`

	LeaderandISRVersionInZK int32     `json:"-"`
	CommitLog               CommitLog `json:"-"`

	Conn io.ReadWriter `json:"-"`
}

func NewPartition(topic string, id int32) *Partition {
	return &Partition{
		ID:    id,
		Topic: topic,
	}
}

func (p *Partition) Delete() error {
	return p.CommitLog.DeleteAll()
}

func (p *Partition) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	return p.CommitLog.NewReader(offset, maxBytes)
}

// Strings returns the topic/Partition as a string.
func (r *Partition) String() string {
	return fmt.Sprintf("%s/%d", r.Topic, r.ID)
}

func (r *Partition) IsOpen() bool {
	return r.CommitLog != nil
}

func (r *Partition) IsLeader(id int32) bool {
	return r.Leader == id
}

func (r *Partition) IsFollowing(id int32) bool {
	for _, b := range r.Replicas {
		if b == id {
			return true
		}
	}
	return false
}

func (p *Partition) HighWatermark() int64 {
	return p.CommitLog.NewestOffset()
}

func (p *Partition) LowWatermark() int64 {
	return p.CommitLog.OldestOffset()
}

func (p *Partition) TruncateTo(offset int64) error {
	return p.CommitLog.TruncateTo(offset)
}

func (p *Partition) Write(b []byte) (int, error) {
	return p.Conn.Write(b)
}

func (p *Partition) Read(b []byte) (int, error) {
	return p.Conn.Read(b)
}

func (p *Partition) Append(ms []byte) (int64, error) {
	return p.CommitLog.Append(ms)
}

func (p *Partition) LeaderID() int32 {
	return p.Leader
}

// Serf manages the cluster membership for Jocko nodes
type Serf interface {
	Bootstrap(node *BrokerConn, reconcileCh chan<- *BrokerConn) error
	Cluster() []*BrokerConn
	Member(memberID int32) *BrokerConn
	Join(addrs ...string) (int, error)
	Leave() error
	Shutdown() error
	Addr() string
}

type RaftCmdType int

type RaftCommand struct {
	Cmd  RaftCmdType      `json:"type"`
	Data *json.RawMessage `json:"data"`
}

// Raft manages consensus for Jocko cluster
type Raft interface {
	Bootstrap(peers []*BrokerConn, fsm raft.FSM, leaderCh chan<- bool) (err error)
	Apply(cmd RaftCommand) error
	IsLeader() bool
	LeaderID() string
	WaitForBarrier() error
	AddPeer(addr string) error
	RemovePeer(addr string) error
	Shutdown() error
	Addr() string
}

type Broker interface {
	ID() int32
	IsController() bool
	CreateTopic(topic string, partitions int32) error
	StartReplica(*Partition) error
	DeleteTopic(topic string) error
	Partition(topic string, id int32) (*Partition, error)
	BrokerConn(brokerID int32) *BrokerConn
	BecomeLeader(topic string, id int32, command *protocol.PartitionState) error
	BecomeFollower(topic string, id int32, command *protocol.PartitionState) error
	Join(addr ...string) (int, error)
	Cluster() []*BrokerConn
	TopicPartitions(topic string) ([]*Partition, error)
	IsLeaderOfPartition(topic string, id int32, leaderID int32) bool
}

type BrokerConn struct {
	ID   int32  `json:"id"`
	Port int    `json:"port"`
	IP   string `json:"addr"`

	SerfPort int               `json:"-"`
	RaftPort int               `json:"-"`
	Status   serf.MemberStatus `json:"-"`

	conn net.Conn
}

func (b *BrokerConn) Addr() *net.TCPAddr {
	return &net.TCPAddr{IP: net.ParseIP(b.IP), Port: b.Port}
}

func (b *BrokerConn) Write(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Write(p)
}

func (b *BrokerConn) Read(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Read(p)
}

func (b *BrokerConn) connect() error {
	addr := &net.TCPAddr{IP: net.ParseIP(b.IP), Port: b.Port}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}
