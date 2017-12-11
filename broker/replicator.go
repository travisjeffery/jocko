package broker

import (
	"fmt"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

// Replicator fetches from the partition's leader producing to itself the follower, thereby replicating the partition.
type Replicator struct {
	logger              log.Logger
	replicaID           int32
	partition           *Partition
	clientID            string
	minBytes            int32
	fetchSize           int32
	maxWaitTime         int32
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	leader              jocko.Client
}

// NewReplicator returns a new replicator instance.
func NewReplicator(logger log.Logger, partition *Partition, replicaID int32, opts ...ReplicatorFn) *Replicator {
	logger = logger.With(
		log.String("ctx", "replicator"),
		log.Int32("id", replicaID),
		log.Int32("partition_id", partition.ID),
		log.Int32("leader_id", partition.LeaderID()),
	)
	r := &Replicator{
		logger:    logger,
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

func (r *Replicator) Replicate() {
	r.logger.Debug("replicate")
	go r.fetchMessages()
	go r.appendMessages()
}

func (r *Replicator) fetchMessages() {
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
			fetchResponse, err := r.leader.FetchMessages(r.clientID, fetchRequest)
			// TODO: probably shouldn't panic. just let this replica fall out of ISR.
			if err != nil {
				r.logger.Error("replicator fetching messages", log.Error("error", err))
				continue
			}
			for _, resp := range fetchResponse.Responses {
				for _, p := range resp.PartitionResponses {
					if p.ErrorCode != protocol.ErrNone.Code() {
						r.logger.Error("replicator partition response", log.Int16("error_code", p.ErrorCode))
						continue
					}
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

func (r *Replicator) appendMessages() {
	for {
		select {
		case <-r.done:
			return
		case msg := <-r.msgs:
			r.logger.Debug("append messages")
			_, err := r.partition.Append(msg)
			if err != nil {
				panic(err)
			}
		}
	}
}

// Close the replicator object when we are no longer following
func (r *Replicator) Close() error {
	r.logger.Debug("close")
	close(r.done)
	return nil
}
