package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/simplelog"
)

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	protocolAddr string
	protocolLn   *net.TCPListener
	httpAddr     string
	httpLn       *net.TCPListener
	logger       *simplelog.Logger
	broker       jocko.Broker
	shutdownCh   chan struct{}
	metrics      *jocko.Metrics
	requestCh    chan jocko.Request
	responseCh   chan jocko.Response
	server       http.Server
}

func New(protocolAddr string, broker jocko.Broker, httpAddr string, metrics *jocko.Metrics, logger *simplelog.Logger) *Server {
	s := &Server{
		protocolAddr: protocolAddr,
		httpAddr:     httpAddr,
		broker:       broker,
		logger:       logger,
		metrics:      metrics,
		shutdownCh:   make(chan struct{}),
		requestCh:    make(chan jocko.Request, 32),
		responseCh:   make(chan jocko.Response, 32),
	}
	return s
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", s.protocolAddr)
	if err != nil {
		return err
	}
	if s.protocolLn, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return err
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", s.httpAddr)
	if err != nil {
		return err
	}
	if s.httpLn, err = net.ListenTCP("tcp", httpAddr); err != nil {
		return err
	}

	// TODO: clean this up. setup metrics via dependency injection or use them
	// through a channel or something.
	r := mux.NewRouter()
	r.Path("/join").Methods("POST").HandlerFunc(s.handleJoin)
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
					s.logger.Debug("listener accept failed: %v", err)
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
					s.logger.Info("failed to write response: %v", err)
				}
			}
		}
	}()

	go s.broker.Run(ctx, s.requestCh, s.responseCh)

	go func() {
		err := s.server.Serve(s.httpLn)
		if err != nil {
			s.logger.Info("serve failed: %v", err)
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
	s.metrics.RequestsHandled.Inc()
	defer conn.Close()

	header := new(protocol.RequestHeader)
	p := make([]byte, 4)

	for {
		err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			s.logger.Info("read deadline failed: %v", err)
			continue
		}
		_, err = io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Info("conn read failed: %v", err)
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
			s.logger.Info("failed to read from connection: %v", err)
			panic(err)
		}

		d := protocol.NewDecoder(b)
		if err := header.Decode(d); err != nil {
			// TODO: handle err
			s.logger.Info("failed to decode header: %v", err)
			panic(err)
		}
		s.logger.Debug("request: correlation id [%d], client id [%s], request size [%d], key [%d]", header.CorrelationID, header.ClientID, size, header.APIKey)

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
			s.logger.Info("failed to decode request: %v", err)
			panic(err)
		}

		s.requestCh <- jocko.Request{
			Header:  header,
			Request: req,
			Conn:    conn,
		}
	}
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	b := new(jocko.ClusterMember)
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// TODO: change join to take a broker
	if err := s.broker.Join(b.IP); err != protocol.ErrNone {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) write(resp jocko.Response) error {
	s.logger.Debug("response: correlation id [%d], key [%d]", resp.Header.CorrelationID, resp.Header.APIKey)
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
