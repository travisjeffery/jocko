package jocko

import (
	"fmt"
	"io"
	"net"

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
	Topic string `json:"topic"`
	ID    int32  `json:"id"`

	// Broker ids
	Replicas        []*BrokerConn `json:"replicas"`
	ISR             []*BrokerConn `json:"isr"`
	Leader          *BrokerConn   `json:"leader"`
	PreferredLeader *BrokerConn   `json:"preferred_leader"`

	LeaderandISRVersionInZK int32     `json:"-"`
	CommitLog               CommitLog `json:"-"`
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
	return int32(r.Leader.ID) == id
}

func (r *Partition) IsFollowing(id int32) bool {
	for _, b := range r.Replicas {
		if int32(b.ID) == id {
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
	return p.Leader.Write(b)
}

func (p *Partition) Read(b []byte) (int, error) {
	return p.Leader.Read(b)
}

func (p *Partition) Append(ms []byte) (int64, error) {
	return p.CommitLog.Append(ms)
}

func (p *Partition) LeaderID() int32 {
	return p.Leader.ID
}

// func (p *Partition) StartReplica(brokerID int32) (err error) {
// 	p.Replicator, err = replicator.NewPartitionReplicator(&replicator.Options{
// 		Partition: p,
// 		ReplicaID: brokerID,
// 	})
// 	return err
// }

type Broker interface {
	ID() int32
	Port() string
	Host() string
	IsController() bool
	CreateTopic(topic string, partitions int32) error
	DeleteTopic(topic string) error
	Partition(topic string, id int32) (*Partition, error)
	BrokerConn(brokerID int32) *BrokerConn
	BecomeLeader(topic string, id int32, command *protocol.PartitionState) error
	BecomeFollower(topic string, id int32, leaderID int32) error
	Join(brokerID int32, addr ...string) (int, error)
	Cluster() []*BrokerConn
	TopicPartitions(topic string) ([]*Partition, error)
	IsLeaderOfPartition(topic string, id int32, leaderID int32) bool
}

type BrokerConn struct {
	ID       int32  `json:"id"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	RaftAddr string `json:"raft_addr"`

	conn net.Conn
}

func (b *BrokerConn) Addr() string {
	return fmt.Sprintf("%s:%s", b.Host, b.Port)
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
	tcpAddr, err := net.ResolveTCPAddr("tcp", b.Addr())
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}
