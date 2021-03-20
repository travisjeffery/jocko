package jocko

import (
	"net"
	"time"

	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
)

func (b *Broker) handleFetchSendFile(ctx *Context, r *protocol.FetchRequest) {
	sp := span(ctx, b.tracer, "fetch")
	defer sp.Finish()
	resp := protocol.Response{
		CorrelationID: ctx.header.CorrelationID,
	}
	conn := ctx.Conn
	fres := &protocol.FetchResponse{
		Responses: make(protocol.FetchTopicResponses, len(r.Topics)),
	}
	msgSetLen := 0
	fres.APIVersion = r.APIVersion
	maxBufSize := 0
	// calculate total length of message set
	//TODO calc max buf length
	maxBufSize = 1024
	for i, topic := range r.Topics {
		fr := &protocol.FetchTopicResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}
		for j, p := range topic.Partitions {
			fpres := &protocol.FetchPartitionResponse{}
			fpres.Partition = p.Partition
			replica, err := b.replicaLookup.Replica(topic.Topic, p.Partition)
			if err != nil {
				panic(err)
			}
			var rdrErr error
			fpres.FileHandle, fpres.SendOffset, fpres.SendSize, rdrErr = replica.Log.SendfileParams(p.FetchOffset, p.MaxBytes)
			if rdrErr != nil {
				panic(rdrErr)
			}
			msgSetLen += fpres.SendSize
			//get length of record
			//
			fr.PartitionResponses[j] = fpres
		}
		fres.Responses[i] = fr
	}
	lenEnc := new(protocol.LenEncoder)
	err := fres.Encode(lenEnc)
	if err != nil {
		panic(err)
	}
	// set length field
	resp.Size = int32(lenEnc.Length + msgSetLen)
	err = sendRes(&resp, maxBufSize, fres, conn)
	if err != nil {
		panic(err)
	}
	return
}
func sendRes(resp *protocol.Response,
	maxSize int,
	r *protocol.FetchResponse,
	conn *net.TCPConn) error {
	b := make([]byte, maxSize)
	e := protocol.NewByteEncoder(b)
	// outer response
	const correlationIDSize = 4
	e.PutInt32(resp.Size + correlationIDSize)
	e.PutInt32(resp.CorrelationID)
	//fetch response
	var err error
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}

	if err = e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, response := range r.Responses {
		if err = e.PutString(response.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(response.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range response.PartitionResponses {
			if err = sendResOfPartition(conn, p, e, r.APIVersion); err != nil {
				return err
			}
		}
	}
	return nil
}
func sendResOfPartition(
	conn *net.TCPConn,
	r *protocol.FetchPartitionResponse,
	e *protocol.ByteEncoder,
	version int16) error {
	e.PutInt32(r.Partition)
	e.PutInt16(r.ErrorCode)
	e.PutInt64(r.HighWatermark)
	var err error
	if version >= 4 {
		e.PutInt64(r.LastStableOffset)

		if err = e.PutArrayLength(len(r.AbortedTransactions)); err != nil {
			return err
		}
		for _, t := range r.AbortedTransactions {
			t.Encode(e)
		}
	}
	e.PutInt32(int32(r.SendSize))
	//log.Info.Println("encoder offset", e.GetOffset())
	conn.Write(e.Bytes()[:e.GetOffset()])
	e.SetOffset(0)
	chunkSize := 4096
	if _, err = commitlog.Sendfile(conn, r.FileHandle, r.SendOffset, r.SendSize, chunkSize); err != nil {
		return err
	}

	return nil
}
