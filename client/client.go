package client

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/travisjeffery/jocko/server"
)

type Options struct {
	Addr string
}

type Client struct {
	Options
	c *http.Client
}

func New(options Options) *Client {
	return &Client{
		Options: options,
		c:       &http.Client{},
	}
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
