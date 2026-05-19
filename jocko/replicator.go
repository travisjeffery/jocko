package jocko

import (
	"time"

	"github.com/cenkalti/backoff"
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
	replica             *Replica
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	leader              client
	backoff             *backoff.ExponentialBackOff
}

type ReplicatorConfig struct {
	MinBytes int32
	// todo: make this a time.Duration
	MaxWaitTime time.Duration
}

// NewReplicator returns a new replicator instance.
func NewReplicator(config ReplicatorConfig, replica *Replica, leader client) *Replicator {
	if config.MinBytes == 0 {
		config.MinBytes = 1
	}
	bo := backoff.NewExponentialBackOff()
	r := &Replicator{
		config:  config,
		replica: replica,
		leader:  leader,
		done:    make(chan struct{}, 2),
		msgs:    make(chan []byte, 2),
		backoff: bo,
	}
	if replica != nil && replica.Log != nil {
		r.offset = replica.Log.NewestOffset()
	}
	return r
}

// Replicate start fetching messages from the leader and appending them to the local commit log.
func (r *Replicator) Replicate() {
	go r.fetchMessages()
}

func (r *Replicator) fetchMessages() {
	var fetchRequest *protocol.FetchRequest
	var fetchResponse *protocol.FetchResponse
	var err error
	for {
		select {
		case <-r.done:
			return
		default:
			fetchRequest = &protocol.FetchRequest{
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
			fetchResponse, err = r.leader.Fetch(fetchRequest)
			// TODO: probably shouldn't panic. just let this replica fall out of ISR.
			if err != nil {
				log.Error.Printf("replicator: fetch messages error: %s", err)
				goto BACKOFF
			}
			for _, resp := range fetchResponse.Responses {
				for _, p := range resp.PartitionResponses {
					if p.ErrorCode != protocol.ErrNone.Code() {
						log.Error.Printf("replicator: partition response error: %d", p.ErrorCode)
						goto BACKOFF
					}
					if len(p.RecordSet) == 0 {
						r.highwaterMarkOffset = p.HighWatermark
						r.backoff.Reset()
						r.sleepEmptyFetch()
						continue
					}
					if err := appendRecordSetEntries(r.replica.Log, p.RecordSet); err != nil {
						log.Error.Printf("replicator: append messages error: %s", err)
						goto BACKOFF
					}
					r.highwaterMarkOffset = p.HighWatermark
					r.offset = r.replica.Log.NewestOffset()
				}
			}

			r.backoff.Reset()
			continue

		BACKOFF:
			time.Sleep(r.backoff.NextBackOff())
		}
	}
}

func appendRecordSetEntries(log CommitLog, recordSet []byte) error {
	for pos := 0; pos < len(recordSet); {
		d := protocol.NewDecoder(recordSet[pos:])
		if _, err := d.Int64(); err != nil {
			return err
		}
		size, err := d.Int32()
		if err != nil {
			return err
		}
		end := pos + d.Offset() + int(size)
		if end > len(recordSet) {
			return protocol.ErrInsufficientData
		}
		if _, err := log.Append(recordSet[pos:end]); err != nil {
			return err
		}
		pos = end
	}
	return nil
}

func (r *Replicator) sleepEmptyFetch() {
	sleep := r.config.MaxWaitTime
	if sleep <= 0 {
		sleep = 10 * time.Millisecond
	}
	time.Sleep(sleep)
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
