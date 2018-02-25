package jocko

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

type ServerConfig struct {
	BrokerAddr string
	HTTPAddr   string
}

// Broker is the interface that wraps the Broker's methods.
type broker interface {
	Run(context.Context, <-chan Request, chan<- Response)
	Shutdown() error
}

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	config     *ServerConfig
	protocolLn *net.TCPListener
	httpLn     *net.TCPListener
	logger     log.Logger
	broker     *Broker
	shutdownCh chan struct{}
	metrics    *Metrics
	requestCh  chan Request
	responseCh chan Response
	tracer     opentracing.Tracer
	server     http.Server
}

func NewServer(config *ServerConfig, broker *Broker, metrics *Metrics, tracer opentracing.Tracer, logger log.Logger) *Server {
	s := &Server{
		config:     config,
		broker:     broker,
		logger:     logger.With(log.Any("config", config)),
		metrics:    metrics,
		shutdownCh: make(chan struct{}),
		requestCh:  make(chan Request, 32),
		responseCh: make(chan Response, 32),
		tracer:     tracer,
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

	httpAddr, err := net.ResolveTCPAddr("tcp", s.config.HTTPAddr)
	if err != nil {
		return err
	}
	if s.httpLn, err = net.ListenTCP("tcp", httpAddr); err != nil {
		return err
	}

	// TODO: clean this up. setup metrics via dependency injection or use them
	// through a channel or something.
	r := mux.NewRouter()
	r.Handle("/metrics", promhttp.Handler())
	r.PathPrefix("").HandlerFunc(s.handleNotFound)

	loggedRouter := handlers.LoggingHandler(os.Stdout, r)
	s.server = http.Server{
		Handler: loggedRouter,
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
				if err := s.write(resp); err != nil {
					s.logger.Error("failed to write response", log.Error("error", err))
				}
			}
		}
	}()

	go s.broker.Run(ctx, s.requestCh, s.responseCh)

	go func() {
		err := s.server.Serve(s.httpLn)
		if err != nil {
			s.logger.Error("serve failed", log.Error("error", err))
		}
	}()

	return nil
}

// Close closes the service.
func (s *Server) Close() {
	close(s.shutdownCh)
	s.server.Close()
	s.httpLn.Close()
	s.protocolLn.Close()
}

func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()

	header := new(protocol.RequestHeader)
	p := make([]byte, 4)

	for {
		err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			s.logger.Error("read deadline failed", log.Error("error", err))
			continue
		}
		_, err = io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Error("conn read failed", log.Error("error", err))
			break
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break // TODO: should this even happen?
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			// TODO: handle request
			s.logger.Error("failed to read from connection", log.Error("error", err))
			panic(err)
		}

		d := protocol.NewDecoder(b)
		if err := header.Decode(d); err != nil {
			// TODO: handle err
			s.logger.Error("failed to decode header", log.Error("error", err))
			panic(err)
		}

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
			// TODO: handle err
			s.logger.Error("failed to decode request", log.Error("error", err))
			panic(err)
		}

		span := s.tracer.StartSpan("request")
		span.SetTag("api_key", header.APIKey)
		span.SetTag("correlation_id", header.CorrelationID)
		span.SetTag("client_id", header.ClientID)
		span.SetTag("size", size)

		ctx := opentracing.ContextWithSpan(context.Background(), span)

		s.requestCh <- Request{
			Ctx:     ctx,
			Header:  header,
			Request: req,
			Conn:    conn,
		}
	}
}

func (s *Server) write(resp Response) error {
	s.logger.Debug("response", log.Int32("correlation id", resp.Header.CorrelationID), log.Int16("api key", resp.Header.APIKey), log.String("request", dump(resp.Response)))
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

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func init() {
	spew.Config.Indent = ""
}

func dump(i interface{}) string {
	return strings.Replace(spew.Sdump(i), "\n", "", -1)
}
