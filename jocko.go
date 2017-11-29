//go:generate mocker --prefix "" --out mock/broker.go --pkg mock . Broker
//go:generate mocker --prefix "" --out mock/commitlog.go --pkg mock . CommitLog
//go:generate mocker --prefix "" --out mock/serf.go --pkg mock . Serf
//go:generate mocker --prefix "" --out mock/raft.go --pkg mock . Raft

package jocko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko/protocol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// CommitLog is the interface that wraps the commit log's methods and
// is used to manage a partition's data.
type CommitLog interface {
	Delete() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
}

// Client is used to request other brokers.
type Client interface {
	FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error)
	CreateTopic(clientID string, createRequest *protocol.CreateTopicRequest) (*protocol.CreateTopicsResponse, error)
	// others
}

type Field = zapcore.Field

type Logger interface {
	// Use Debug if you want your logs running in development, testing,
	// production.
	Debug(msg string, fields ...Field)
	// Use Info if you want your logs running in production.
	Info(msg string, fields ...Field)
	// Use Error if you want your logs running in production and you have an error.
	Error(msg string, fields ...Field)
}

func String(key string, val string) Field {
	return zap.String(key, val)
}

func Int(key string, val int) Field {
	return zap.Int(key, val)
}

func Int16(key string, val int16) Field {
	return zap.Int16(key, val)
}

func Int32(key string, val int32) Field {
	return zap.Int32(key, val)
}

func Uint32(key string, val uint32) Field {
	return zap.Uint32(key, val)
}

func Duration(key string, val time.Duration) Field {
	return zap.Duration(key, val)
}

func Error(key string, val error) Field {
	return zap.NamedError(key, val)
}

func Any(key string, val interface{}) Field {
	return zap.Any(key, val)
}

// Alias prometheus' counter, probably only need to use Inc() though.
type Counter = prometheus.Counter

// Metrics is used for tracking metrics.
type Metrics struct {
	RequestsHandled Counter
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
	// Conn is a connection to the broker that is this partition's leader, used for replication.
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
	return p.CommitLog.Delete()
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

// Truncate is used to truncate the partition's logs before the given offset.
func (p *Partition) Truncate(offset int64) error {
	return p.CommitLog.Truncate(offset)
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

// Different possible states of serf member.
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
