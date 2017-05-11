package jocko

import (
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/travisjeffery/jocko/protocol"
)

// CommitLog is the interface that wraps the commit log's methods and
// is used to manage a partition's data.
type CommitLog interface {
	DeleteAll() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	TruncateTo(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
}

// Proxy is the interface that wraps Proxy methods for forwarding requests
// to an existing Jocko server and returning server response to caller
type Proxy interface {
	FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error)
	CreateTopic(clientID string, createRequest *protocol.CreateTopicRequest) (*protocol.CreateTopicsResponse, error)
	// others
}

// Partition is the unit of storage in Jocko.
type Partition struct {
	Topic           string  `json:"topic"`
	ID              int32   `json:"id"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	Leader          int32   `json:"leader"`
	PreferredLeader int32   `json:"preferred_leader"`

	LeaderAndISRVersionInZK int32     `json:"-"`
	CommitLog               CommitLog `json:"-"`

	Conn io.ReadWriter `json:"-"`
}

// NewPartition is used to create a new partition.
func NewPartition(topic string, id int32) *Partition {
	return &Partition{
		ID:    id,
		Topic: topic,
	}
}

// Delete is used to delete the partition's data/commitlog.
func (p *Partition) Delete() error {
	return p.CommitLog.DeleteAll()
}

// NewReader is used to create a reader at the given offset and will
// read up to maxBytes.
func (p *Partition) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	return p.CommitLog.NewReader(offset, maxBytes)
}

// String returns the topic/Partition as a string.
func (r *Partition) String() string {
	return fmt.Sprintf("%s/%d", r.Topic, r.ID)
}

// IsOpen is used to check whether the partition's commit log has been
// initialized.
func (r *Partition) IsOpen() bool {
	return r.CommitLog != nil
}

// IsLeader is used to check if the given broker ID's the partition's
// leader.
func (r *Partition) IsLeader(id int32) bool {
	return r.Leader == id
}

// IsFollowing is used to check if the given broker ID's should
// follow/replicate the leader.
func (r *Partition) IsFollowing(id int32) bool {
	for _, b := range r.Replicas {
		if b == id {
			return true
		}
	}
	return false
}

// HighWatermark is used to get the newest offset of the partition.
func (p *Partition) HighWatermark() int64 {
	return p.CommitLog.NewestOffset()
}

// LowWatermark is used to oldest offset of the partition.
func (p *Partition) LowWatermark() int64 {
	return p.CommitLog.OldestOffset()
}

// TruncateTo is used to truncate the partition's logs before the given offset.
func (p *Partition) TruncateTo(offset int64) error {
	return p.CommitLog.TruncateTo(offset)
}

// Write is used to directly write the given bytes to the partition's leader.
func (p *Partition) Write(b []byte) (int, error) {
	return p.Conn.Write(b)
}

// Write is used to directly read the given bytes from the partition's leader.
func (p *Partition) Read(b []byte) (int, error) {
	return p.Conn.Read(b)
}

// Append is used to append message sets to the partition.
func (p *Partition) Append(ms []byte) (int64, error) {
	return p.CommitLog.Append(ms)
}

// LeaderID is used to get the partition's leader broker ID.
func (p *Partition) LeaderID() int32 {
	return p.Leader
}

// MemberStatus is the state that a member is in.
type MemberStatus int

// Different possible states of serf member
const (
	StatusNone MemberStatus = iota
	StatusAlive
	StatusLeaving
	StatusLeft
	StatusFailed
	StatusReap
)

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

type RaftCmdType int

type RaftCommand struct {
	Cmd  RaftCmdType      `json:"type"`
	Data *json.RawMessage `json:"data"`
}

// Raft is the interface that wraps Raft's methods and is used to
// manage consensus for the Jocko cluster.
type Raft interface {
	Bootstrap(serf Serf, serfEventCh <-chan *ClusterMember, commandCh chan<- RaftCommand) error
	Apply(cmd RaftCommand) error
	IsLeader() bool
	LeaderID() string
	Shutdown() error
	Addr() string
}

// Broker is the interface that wraps the Broker's methods.
type Broker interface {
	ID() int32
	IsController() bool
	CreateTopic(topic string, partitions int32) error
	StartReplica(*Partition) error
	DeleteTopic(topic string) error
	Partition(topic string, id int32) (*Partition, error)
	ClusterMember(brokerID int32) *ClusterMember
	BecomeLeader(topic string, id int32, command *protocol.PartitionState) error
	BecomeFollower(topic string, id int32, command *protocol.PartitionState) error
	Join(addr ...string) (int, error)
	Cluster() []*ClusterMember
	TopicPartitions(topic string) ([]*Partition, error)
	IsLeaderOfPartition(topic string, id int32, leaderID int32) bool
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

func (b *ClusterMember) connect() error {
	addr := &net.TCPAddr{IP: net.ParseIP(b.IP), Port: b.Port}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}
