package jocko

import (
	"context"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko/protocol"
)

type CommitLog interface {
	Delete() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
}

// Client is used to request other brokers.
type Client interface {
	FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error)
	CreateTopics(clientID string, createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error)
	LeaderAndISR(clientID string, request *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error)
	// others
}

// Alias prometheus' counter, probably only need to use Inc() though.
type Counter = prometheus.Counter

// Metrics is used for tracking metrics.
type Metrics struct {
	RequestsHandled Counter
}

// Request represents an API request.
type Request struct {
	Conn    io.ReadWriter
	Header  *protocol.RequestHeader
	Request interface{}
}

// Request represents an API request.
type Response struct {
	Conn     io.ReadWriter
	Header   *protocol.RequestHeader
	Response interface{}
}

// Broker is the interface that wraps the Broker's methods.
type Broker interface {
	Run(context.Context, <-chan Request, chan<- Response)
	Shutdown() error
}

//go:generate mocker --prefix "" --out mock/broker.go --pkg mock . Broker
//go:generate mocker --prefix "" --out mock/commitlog.go --pkg mock . CommitLog
