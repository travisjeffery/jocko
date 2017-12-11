package broker

import (
	"fmt"
	"io"

	"github.com/travisjeffery/jocko/commitlog"
)

// Partition is the unit of storage in Jocko.
type Partition struct {
	Topic                   string               `json:"topic"`
	ID                      int32                `json:"id"`
	Replicas                []int32              `json:"replicas"`
	ISR                     []int32              `json:"isr"`
	Leader                  int32                `json:"leader"`
	PreferredLeader         int32                `json:"preferred_leader"`
	LeaderAndISRVersionInZK int32                `json:"-"`
	CommitLog               *commitlog.CommitLog `json:"-"`
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
