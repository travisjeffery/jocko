package testutil

import (
	"bytes"
	"io"
	"math/rand"
	"net"

	"github.com/travisjeffery/jocko/protocol"
)

func CreateTopic(conn net.Conn, request *protocol.CreateTopicRequest) error {
	buf := new(bytes.Buffer)
	var header protocol.Response
	var body protocol.Body

	body = &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{request},
	}
	var req protocol.Encoder = &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      "createtopic",
		Body:          body,
	}

	b, err := protocol.Encode(req)
	if err != nil {
		return err
	}

	_, err = conn.Write(b)
	if err != nil {
		return err
	}

	_, err = io.CopyN(buf, conn, 8)
	if err != nil {
		return err
	}

	err = protocol.Decode(buf.Bytes(), &header)
	if err != nil {
		return err
	}

	buf.Reset()
	_, err = io.CopyN(buf, conn, int64(header.Size-4))
	if err != nil {
		return err
	}

	createTopicsResponse := &protocol.CreateTopicsResponse{}
	err = protocol.Decode(buf.Bytes(), createTopicsResponse)

	return err
}
