// Package replicator provides the Replicator which fetches
// from the partition's leader and produces to a follower thereby
// replicating the partition.

package broker

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"

	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

type Replicator struct {
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
}

func NewReplicator(partition *jocko.Partition, replicaID int32, opts ...ReplicatorFn) *Replicator {
	r := &Replicator{
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
	go r.fetchMessages()
	go r.writeMessages()
}

func (r *Replicator) fetchMessages() {
	for {
		select {
		case <-r.done:
			return
		default:
			fetchBody := &protocol.FetchRequest{
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
			var req protocol.Encoder = &protocol.Request{
				CorrelationID: rand.Int31(),
				ClientID:      r.clientID,
				Body:          fetchBody,
			}
			b, err := protocol.Encode(req)
			if err != nil {
				panic(err)
			}
			_, err = r.partition.Write(b)
			if err != nil {
				panic(err)
			}
			var header protocol.Response
			br := bytes.NewBuffer(make([]byte, 0, 8))
			if _, err = io.CopyN(br, r.partition, 8); err != nil {
				panic(err)
			}
			if err = protocol.Decode(br.Bytes(), &header); err != nil {
				panic(err)
			}
			c := make([]byte, 0, header.Size-4)
			cr := bytes.NewBuffer(c)
			_, err = io.CopyN(cr, r.partition, int64(header.Size-4))
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

func (r *Replicator) writeMessages() {
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

func (pr *Replicator) Close() error {
	close(pr.done)
	return nil
}
