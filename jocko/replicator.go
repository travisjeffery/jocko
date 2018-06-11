package jocko

import (
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

// Client is used to request other brokers.
type client interface {
	Fetch(fetchRequest *protocol.FetchRequest) (*protocol.FetchResponse, error)
	CreateTopics(createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error)
	LeaderAndISR(request *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error)
	// others
}

// Replicator fetches from the partition's leader producing to itself the follower, thereby replicating the partition.
type Replicator struct {
	config              ReplicatorConfig
	logger              log.Logger
	replica             *Replica
	fetchSize           int32
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	leader              client
}

type ReplicatorConfig struct {
	MinBytes int32
	// todo: make this a time.Duration
	MaxWaitTime int32
}

// NewReplicator returns a new replicator instance.
func NewReplicator(config ReplicatorConfig, replica *Replica, leader client, logger log.Logger) *Replicator {
	if config.MinBytes == 0 {
		config.MinBytes = 1
	}
	r := &Replicator{
		config:  config,
		logger:  logger,
		replica: replica,
		leader:  leader,
		done:    make(chan struct{}, 2),
		msgs:    make(chan []byte, 2),
	}
	return r
}

// Replicate start fetching messages from the leader and appending them to the local commit log.
func (r *Replicator) Replicate() {
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
				ReplicaID:   r.replica.BrokerID,
				MaxWaitTime: r.config.MaxWaitTime,
				MinBytes:    r.config.MinBytes,
				Topics: []*protocol.FetchTopic{{
					Topic: r.replica.Partition.Topic,
					Partitions: []*protocol.FetchPartition{{
						Partition:   r.replica.Partition.ID,
						FetchOffset: r.offset,
					}},
				}},
			}
			fetchResponse, err := r.leader.Fetch(fetchRequest)
			// TODO: probably shouldn't panic. just let this replica fall out of ISR.
			if err != nil {
				r.logger.Error("failed to fetch messages", log.Error("error", err))
				continue
			}
			for _, resp := range fetchResponse.Responses {
				for _, p := range resp.PartitionResponses {
					if p.ErrorCode != protocol.ErrNone.Code() {
						r.logger.Error("partition response error", log.Int16("error code", p.ErrorCode), log.Any("response", p))
						continue
					}
					if p.RecordSet == nil {
						// r.logger.Debug("replicator: fetch messages: record set is nil")
						continue
					}
					offset := int64(protocol.Encoding.Uint64(p.RecordSet[:8]))
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
			_, err := r.replica.Log.Append(msg)
			if err != nil {
				panic(err)
			}
		}
	}
}

// Close the replicator object when we are no longer following
func (r *Replicator) Close() error {
	close(r.done)
	return nil
}
