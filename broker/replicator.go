package broker

import (
	"fmt"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

// replicator fetches from the partition's leader and produces to a follower
// thereby replicating the partition
type replicator struct {
	replicaID           int32
	partition           *jocko.Partition
	clientID            string
	minBytes            int32
	fetchSize           int32
	maxWaitTime         int32
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	proxy               jocko.Proxy
}

// newReplicator returns a new replicator object
func newReplicator(partition *jocko.Partition, replicaID int32, opts ...ReplicatorFn) *replicator {
	r := &replicator{
		partition: partition,
		replicaID: replicaID,
		clientID:  fmt.Sprintf("Replicator-%d", replicaID),
		done:      make(chan struct{}, 2),
		msgs:      make(chan []byte, 2),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// replicate starts replication process of fetching and writing messages
func (r *replicator) replicate() {
	go r.fetchMessages()
	go r.writeMessages()
}

func (r *replicator) fetchMessages() {
	for {
		select {
		case <-r.done:
			return
		default:
			fetchRequest := &protocol.FetchRequest{
				ReplicaID:   r.replicaID,
				MaxWaitTime: r.maxWaitTime,
				MinBytes:    r.minBytes,
				Topics: []*protocol.FetchTopic{{
					Topic: r.partition.Topic,
					Partitions: []*protocol.FetchPartition{{
						Partition:   r.partition.ID,
						FetchOffset: r.offset,
					}},
				}},
			}
			fetchResponse, err := r.proxy.FetchMessages(r.clientID, fetchRequest)
			if err != nil {
				panic(err)
			}
			for _, resp := range fetchResponse.Responses {
				for _, p := range resp.PartitionResponses {
					offset := int64(protocol.Encoding.Uint64(p.RecordSet[:8])) + 1
					if offset > r.offset {
						r.msgs <- p.RecordSet
						r.highwaterMarkOffset = p.HighWatermark
						r.offset = offset
					}
				}
			}
		}
	}
}

func (r *replicator) writeMessages() {
	for {
		select {
		case <-r.done:
			return
		case msg := <-r.msgs:
			_, err := r.partition.Append(msg)
			if err != nil {
				panic(err)
			}
		}
	}
}

// close the replicator object when we are no longer following
func (r *replicator) close() error {
	close(r.done)
	return nil
}
