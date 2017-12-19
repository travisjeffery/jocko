package server

import (
	"bytes"
	"io"
	"math/rand"

	"github.com/travisjeffery/jocko/protocol"
)

// Client is used to connect and request to other brokers, for example for replication.
type Client struct {
	conn io.ReadWriter
}

// NewClient creates a new client to a Jocko server that can be reached over conn.
func NewClient(conn io.ReadWriter) *Client {
	return &Client{
		conn: conn,
	}
}

// makeRequest sends request req to server.
// Server response is given to decoder to decode it as per request expectations
func (p *Client) makeRequest(req *protocol.Request, decoder protocol.Decoder) error {
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
func (p *Client) FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error) {
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
func (p *Client) CreateTopics(clientID string, createRequests *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	req := &protocol.Request{
		CorrelationID: rand.Int31(),
		ClientID:      clientID,
		Body:          createRequests,
	}
	createResponse := new(protocol.CreateTopicsResponse)
	if err := p.makeRequest(req, createResponse); err != nil {
		return nil, err
	}
	return createResponse, nil
}
