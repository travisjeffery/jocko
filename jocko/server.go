package jocko

import (
	"context"
	"io"
	"net"
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/util"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

type contextKey string

var (
	serverVerboseLogs    bool
	requestQueueSpanKey  = contextKey("request queue span key")
	responseQueueSpanKey = contextKey("response queue span key")
)

func init() {
	spew.Config.Indent = ""

	e := os.Getenv("JOCKODEBUG")
	if strings.Contains(e, "server=1") {
		serverVerboseLogs = true
	}
}

// Broker is the interface that wraps the Broker's methods.
type broker interface {
	Run(context.Context, <-chan Request, chan<- Response)
	Shutdown() error
}

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	config     *config.ServerConfig
	protocolLn *net.TCPListener
	logger     log.Logger
	broker     *Broker
	shutdownCh chan struct{}
	metrics    *Metrics
	requestCh  chan Request
	responseCh chan Response
	tracer     opentracing.Tracer
	close      func() error
}

func NewServer(config *config.ServerConfig, broker *Broker, metrics *Metrics, tracer opentracing.Tracer, close func() error, logger log.Logger) *Server {
	s := &Server{
		config:     config,
		broker:     broker,
		logger:     logger.With(log.Int32("node id", broker.config.ID), log.String("addr", broker.config.Addr)),
		metrics:    metrics,
		shutdownCh: make(chan struct{}),
		requestCh:  make(chan Request, 32),
		responseCh: make(chan Response, 32),
		tracer:     tracer,
		close:      close,
	}
	s.logger.Info("hello")
	return s
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", s.config.BrokerAddr)
	if err != nil {
		return err
	}
	if s.protocolLn, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			default:
				conn, err := s.protocolLn.Accept()
				if err != nil {
					s.logger.Error("listener accept failed", log.Error("error", err))
					continue
				}

				go s.handleRequest(conn)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			case resp := <-s.responseCh:
				if queueSpan, ok := resp.Ctx.Value(responseQueueSpanKey).(opentracing.Span); ok {
					queueSpan.Finish()
				}
				if err := s.handleResponse(resp); err != nil {
					s.logger.Error("failed to write response", log.Error("error", err))
				}
			}
		}
	}()

	go s.broker.Run(ctx, s.requestCh, s.responseCh)

	return nil
}

// Close closes the service.
func (s *Server) Close() {
	// close(s.shutdownCh)
	s.broker.Shutdown()
	s.protocolLn.Close()
	s.close()
}

func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()

	header := new(protocol.RequestHeader)
	p := make([]byte, 4)

	for {
		_, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Error("conn read failed", log.Error("error", err))
			break
		}

		span := s.tracer.StartSpan("request")
		decodeSpan := s.tracer.StartSpan("server: decode request", opentracing.ChildOf(span.Context()))

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break // TODO: should this even happen?
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			// TODO: handle request
			span.LogKV("msg", "failed to read from connection", "err", err)
			span.Finish()
			panic(err)
		}

		d := protocol.NewDecoder(b)
		if err := header.Decode(d); err != nil {
			// TODO: handle err
			span.LogKV("msg", "failed to decode header", "err", err)
			span.Finish()
			panic(err)
		}

		span.SetTag("api_key", header.APIKey)
		span.SetTag("correlation_id", header.CorrelationID)
		span.SetTag("client_id", header.ClientID)
		span.SetTag("size", size)
		span.SetTag("node_id", s.broker.config.ID) // can I set this globally for the tracer?

		var req protocol.Decoder
		switch header.APIKey {
		case protocol.APIVersionsKey:
			req = &protocol.APIVersionsRequest{}
		case protocol.ProduceKey:
			req = &protocol.ProduceRequest{}
		case protocol.FetchKey:
			req = &protocol.FetchRequest{}
		case protocol.OffsetsKey:
			req = &protocol.OffsetsRequest{}
		case protocol.MetadataKey:
			req = &protocol.MetadataRequest{}
		case protocol.CreateTopicsKey:
			req = &protocol.CreateTopicRequests{}
		case protocol.DeleteTopicsKey:
			req = &protocol.DeleteTopicsRequest{}
		case protocol.LeaderAndISRKey:
			req = &protocol.LeaderAndISRRequest{}
		}

		if err := req.Decode(d); err != nil {
			s.logger.Error("failed to decode request", log.Error("err", err), log.Any("header", header))
			span.LogKV("msg", "failed to decode request", "err", err)
			span.Finish()
			panic(err)
		}

		decodeSpan.Finish()

		s.vlog(span, "request", req)

		ctx := opentracing.ContextWithSpan(context.Background(), span)

		queueSpan := s.tracer.StartSpan("server: queue request", opentracing.ChildOf(span.Context()))
		ctx = context.WithValue(ctx, requestQueueSpanKey, queueSpan)

		s.requestCh <- Request{
			Ctx:     ctx,
			Header:  header,
			Request: req,
			Conn:    conn,
		}
	}
}

func (s *Server) handleResponse(resp Response) error {
	psp := opentracing.SpanFromContext(resp.Ctx)
	sp := s.tracer.StartSpan("server: handle response", opentracing.ChildOf(psp.Context()))
	s.vlog(sp, "response", resp.Response)
	defer psp.Finish()
	defer sp.Finish()
	b, err := protocol.Encode(resp.Response.(protocol.Encoder))
	if err != nil {
		return err
	}
	_, err = resp.Conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.protocolLn.Addr()
}

func (s *Server) ID() int32 {
	return s.broker.config.ID
}

func (s *Server) vlog(span opentracing.Span, k string, i interface{}) {
	if serverVerboseLogs {
		span.LogKV(k, util.Dump(i))
	}
}
