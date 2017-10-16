package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/simplelog"
)

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	addr      string
	ln        *net.TCPListener
	mu        sync.Mutex
	logger    *simplelog.Logger
	broker    jocko.Broker
	shutdownc chan struct{}
	metrics   *serverMetrics
	requestc  chan jocko.Request
	responsec chan jocko.Response
}

func New(addr string, broker jocko.Broker, logger *simplelog.Logger, r prometheus.Registerer) *Server {
	s := &Server{
		addr:      addr,
		broker:    broker,
		logger:    logger,
		shutdownc: make(chan struct{}),
		requestc:  make(chan jocko.Request, 32),
		responsec: make(chan jocko.Response, 32),
	}
	s.metrics = newServerMetrics(r)
	return s
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		panic(err)
	}

	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln

	r := mux.NewRouter()
	r.Methods("POST").Path("/join").HandlerFunc(s.handleJoin)
	r.PathPrefix("").HandlerFunc(s.handleNotFound)
	http.Handle("/", r)

	loggedRouter := handlers.LoggingHandler(os.Stdout, r)

	server := http.Server{
		Handler: loggedRouter,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownc:
				break
			default:
				conn, err := s.ln.Accept()
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
			case <-s.shutdownc:
				break
			case resp := <-s.responsec:
				if err := s.write(resp); err != nil {
					s.logger.Info("failed to write response: %v", err)
				}
			}
		}
	}()

	go s.broker.Run(ctx, s.requestc, s.responsec)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			s.logger.Info("serve failed: %v", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Server) Close() {
	close(s.shutdownc)
	s.ln.Close()
	return
}

func (s *Server) handleRequest(conn net.Conn) {
	s.metrics.requestsHandled.Inc()
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

		s.requestc <- jocko.Request{
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
	return s.ln.Addr()
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}
