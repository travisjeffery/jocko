// Package replicator provides the PartitionReplicator which fetches
// from the partition's leader and produces to a follower thereby
// replicating the partition.

package replicator

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"

	"github.com/travisjeffery/jocko/cluster"
	"github.com/travisjeffery/jocko/protocol"
)

type Options struct {
	Partition   *cluster.TopicPartition
	ReplicaID   int32 // broker id of the follower
	FetchSize   int32
	MinBytes    int32
	MaxWaitTime int32
}

func (o *Options) clientID() string {
	return fmt.Sprintf("Partition Replicator for Broker/Topic/Partition: [%d/%s/%d]",
		o.Partition.Leader.ID,
		o.Partition.Topic,
		o.Partition.Partition)
}

type PartitionReplicator struct {
	*Options
	highwaterMarkOffset int64
	clientID            string
	offset              int64
	msgs                chan []byte
	done                chan struct{}
}

func NewPartitionReplicator(options *Options) (*PartitionReplicator, error) {
	return &PartitionReplicator{
		Options:  options,
		clientID: options.clientID(),
		done:     make(chan struct{}, 2),
		msgs:     make(chan []byte, 2),
	}, nil
}

func (r *PartitionReplicator) Replicate() error {
	hw := r.Partition.CommitLog.NewestOffset()
	err := r.Partition.CommitLog.TruncateTo(hw)
	if err != nil {
		return err
	}
	go r.fetchMessages()
	go r.writeMessages()
	return nil
}

func (r *PartitionReplicator) fetchMessages() {
	for {
		select {
		case <-r.done:
			return
		default:
			fetchBody := &protocol.FetchRequest{
				ReplicaID:   r.ReplicaID,
				MaxWaitTime: r.MaxWaitTime,
				MinBytes:    r.MinBytes,
				Topics: []*protocol.FetchTopic{{
					Topic: r.Partition.Topic,
					Partitions: []*protocol.FetchPartition{{
						Partition:   r.Partition.Partition,
						FetchOffset: r.offset,
					}},
				}},
			}
			var req protocol.Encoder = &protocol.Request{
				CorrelationID: rand.Int31(),
				ClientID:      r.clientID,
				Body:          fetchBody,
			}
			b, err := protocol.Encode(req)
			if err != nil {
				panic(err)
			}
			_, err = r.Partition.Leader.Write(b)
			if err != nil {
				panic(err)
			}
			var header protocol.Response
			br := bytes.NewBuffer(make([]byte, 0, 8))
			if _, err = io.CopyN(br, r.Partition.Leader, 8); err != nil {
				panic(err)
			}
			if err = protocol.Decode(br.Bytes(), &header); err != nil {
				panic(err)
			}
			c := make([]byte, 0, header.Size-4)
			cr := bytes.NewBuffer(c)
			_, err = io.CopyN(cr, r.Partition.Leader, int64(header.Size-4))
			if err != nil {
				panic(err)
			}
			fetchResponse := new(protocol.FetchResponses)
			err = protocol.Decode(cr.Bytes(), fetchResponse)
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

func (r *PartitionReplicator) writeMessages() {
	for {
		select {
		case <-r.done:
			return
		case msg := <-r.msgs:
			_, err := r.Partition.CommitLog.Append(msg)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (pr *PartitionReplicator) Close() error {
	close(pr.done)
	return nil
}
