package jocko

import (
	"context"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko/protocol"
)

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
	CreateTopics(clientID string, createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error)
	// others
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
	if p.CommitLog != nil {
		return p.CommitLog.Delete()
	}
	return nil
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
	Shutdown() error
}

//go:generate mocker --prefix "" --out mock/broker.go --pkg mock . Broker
//go:generate mocker --prefix "" --out mock/commitlog.go --pkg mock . CommitLog
