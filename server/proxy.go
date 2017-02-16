package server

import (
	"bytes"
	"io"
	"math/rand"

	"github.com/travisjeffery/jocko/protocol"
)

// Proxy acts as a proxy for an existing Jocko server
// It forwards requests to server over tcp connection and returns server response to caller
type Proxy struct {
	conn io.ReadWriter
}

// NewProxy creates a new proxy to a Jocko server that can be reached over conn
func NewProxy(conn io.ReadWriter) *Proxy {
	return &Proxy{
		conn: conn,
	}
}

// makeRequest sends request req to server.
// Server response is given to decoder to decode it as per request expectations
func (p *Proxy) makeRequest(req *protocol.Request, decoder protocol.Decoder) error {
	b, err := protocol.Encode(req)
	if err != nil {
		return err
	}
	if _, err = p.conn.Write(b); err != nil {
		return err
	}
	br := bytes.NewBuffer(make([]byte, 0, 8))
	if _, err = io.CopyN(br, p.conn, 8); err != nil {
		return err
	}
	var header protocol.Response
	if err = protocol.Decode(br.Bytes(), &header); err != nil {
		return err
	}
	c := make([]byte, 0, header.Size-4)
	buffer := bytes.NewBuffer(c)
	if _, err = io.CopyN(buffer, p.conn, int64(header.Size-4)); err != nil {
		return err
	}
	if err = protocol.Decode(buffer.Bytes(), decoder); err != nil {
		return err
	}
	return nil
}

// FetchMessages of topics from server as per fetchRequest
func (p *Proxy) FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error) {
	req := &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      clientID,
		Body:          fetchRequest,
	}
	fetchResponse := new(protocol.FetchResponses)
	if err := p.makeRequest(req, fetchResponse); err != nil {
		return nil, err
	}
	return fetchResponse, nil
}

// CreateTopic sends request to server to create a topic as per createRequest
func (p *Proxy) CreateTopic(clientID string, createRequest *protocol.CreateTopicRequest) (*protocol.CreateTopicsResponse, error) {
	body := &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{createRequest},
	}
	req := &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      clientID,
		Body:          body,
	}
	createResponse := new(protocol.CreateTopicsResponse)
	if err := p.makeRequest(req, createResponse); err != nil {
		return nil, err
	}
	return createResponse, nil
}
