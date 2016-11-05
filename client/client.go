package client

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/travisjeffery/jocko/cluster"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/server"
)

type Options struct {
	Addr string
}

type Client struct {
	Options
	c *http.Client
}

type request struct {
	// ID of the API (e.g. produce, fetch, metadata)
	APIKey int16
	// Version of the API to use
	APIVersion int16
	// User defined ID to correlate requests between server and client
	CorrelationID int32
	// User defined ID that identifies the client
	ClientID       string
	RequestMessage []byte
}

func (c *Client) Metadata(topics ...string) (metadata server.MetadataResponse, err error) {
	req := server.MetadataRequest{Topics: topics}
	b := new(bytes.Buffer)
	if err = json.NewEncoder(b).Encode(req); err != nil {
		return
	}
	res, err := c.c.Post(c.Addr+"/metadata", "application/json", b)
	if err != nil {
		return metadata, err
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&metadata); err != nil {
		return metadata, err
	}
	return metadata, nil
}

func (c *Client) Produce(partition cluster.TopicPartition, messageSet *commitlog.MessageSet) (resp server.ProduceResponse, err error) {
	req := Request{
		APIKey:     0,
		APIVersion: 2,
	}
}

func (c *Client) correlationID() int32 {

}

func (c *Client) clientID() string {

}
