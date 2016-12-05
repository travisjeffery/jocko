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
	FetchSize   int32
	MinBytes    int32
	MaxWaitTime int32
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
	clientID := fmt.Sprintf("Partition Replicator for Broker/Topic/Partition: [%d/%s/%d]", options.Partition.Leader.ID, options.Partition.Topic, options.Partition.Partition)
	return &PartitionReplicator{
		Options:  options,
		clientID: clientID,
		done:     make(chan struct{}, 2),
		msgs:     make(chan []byte, 2),
	}, nil
}

func (r *PartitionReplicator) Replicate() {
	go r.fetchMessages()
	go r.writeMessages()
}

func (r *PartitionReplicator) fetchMessages() error {
	for {
		select {
		case <-r.done:
			return nil
		default:
			fetchBody := &protocol.FetchRequest{
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
				return err
			}
			_, err = r.Partition.Leader.Write(b)
			if err != nil {
				return err
			}

			var header protocol.Response
			br := bytes.NewBuffer(make([]byte, 0, 8))
			if _, err = io.CopyN(br, r.Partition.Leader, 8); err != nil {
				return err
			}
			if err = protocol.Decode(br.Bytes(), &header); err != nil {
				return err
			}
			c := make([]byte, 0, header.Size-4)
			cr := bytes.NewBuffer(c)
			_, err = io.CopyN(cr, r.Partition.Leader, int64(header.Size-4))

			fetchResponse := new(protocol.FetchResponses)
			err = protocol.Decode(cr.Bytes(), fetchResponse)
			if err != nil {
				return err
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

func (r *PartitionReplicator) writeMessages() error {
	for {
		select {
		case <-r.done:
			return nil
		case msg := <-r.msgs:
			_, err := r.Partition.CommitLog.Append(msg)
			if err != nil {
				return err
			}
		}
	}
}

func (pr *PartitionReplicator) Close() error {
	close(pr.done)
	return nil
}
