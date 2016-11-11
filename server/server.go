package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/cluster"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
)

type Broker struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port string `json:"port"`
}

type Server struct {
	addr string
	ln   *net.TCPListener

	logger *log.Logger
	broker *broker.Broker
}

func New(addr string, broker *broker.Broker) *Server {
	logger := log.New(os.Stderr, "server", log.LstdFlags)
	return &Server{
		addr:   addr,
		broker: broker,
		logger: logger,
	}
}

// Start starts the service.
func (s *Server) Start() error {
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
		conn, err := s.ln.Accept()
		if err != nil {
			s.logger.Fatalf("Listener accept failed: %s", err)
		}

		go s.handleRequest(conn)
	}()

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			s.logger.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Server) Close() {
	s.ln.Close()
	return
}

func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()

	header := new(protocol.RequestHeader)
	p := make([]byte, 4)

	for {
		err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			s.logger.Printf("read deadline failed: %s", err)
			continue
		}

		n, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if n == 0 || err != nil {
			// TODO: handle err
			s.logger.Printf("Conn read failed: %s", err)
			continue
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			continue
		}
		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		_, err = io.ReadFull(conn, b[4:])
		if err != nil {
			panic(err)
		}

		d := protocol.NewDecoder(b)
		header.Decode(d)

		switch header.APIKey {
		case 0:
			req := &protocol.ProduceRequest{}
			req.Decode(d)
			if err = s.handleProduce(conn, header, req); err != nil {
				s.logger.Printf("produce failed: %s", err)
			}
		case 1:
			req := &protocol.FetchRequest{}
			req.Decode(d)
			if err = s.handleFetch(conn, header, req); err != nil {
				s.logger.Printf("fetch failed: %s", err)
			}
		case 3:
			req := &protocol.MetadataRequest{}
			req.Decode(d)
			if err = s.handleMetadata(conn, header, req); err != nil {
				s.logger.Printf("metadata failed: %s", err)
			}
		case 19:
			req := &protocol.CreateTopicRequests{}
			req.Decode(d)
			if err = s.handleCreateTopic(conn, header, req); err != nil {
				s.logger.Printf("create topic failed: %s", err)
			}
		}
	}
}

func (s *Server) handleCreateTopic(conn net.Conn, header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) (err error) {
	resp := new(protocol.CreateTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))

	if s.broker.IsController() {
		for i, req := range reqs.Requests {
			err = s.broker.CreateTopic(req.Topic, req.NumPartitions)
			if err != nil {
				s.logger.Printf("[ERR] jocko: Failed to create topic %s: %v", req.Topic, err)
				return
			}
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNone,
			}
		}
	} else {
		// cID := s.broker.ControllerID()
		// send the request to the controller
		return
	}
	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}

	b, err := protocol.Encode(r)

	if err != nil {
		return err
	}

	_, err = conn.Write(b)

	return err
}

func zero(p []byte) {
	for i := range p {
		p[i] = 0
	}
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.broker.Join(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleMetadata(conn net.Conn, header *protocol.RequestHeader, req *protocol.MetadataRequest) error {
	brokerIDs, err := s.broker.Brokers()
	brokers := make([]*protocol.Broker, len(brokerIDs))
	topics := make([]*protocol.TopicMetadata, len(req.Topics))
	for i, bID := range brokerIDs {
		host, port, err := net.SplitHostPort(bID)
		if err != nil {
			return err
		}
		// TODO: replace this with an actual id
		nodeID := i
		if err != nil {
			return err
		}
		nPort, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		brokers[i] = &protocol.Broker{
			NodeID: int32(nodeID),
			Host:   host,
			Port:   int32(nPort),
		}
	}
	for i, t := range req.Topics {
		partitions, err := s.broker.PartitionsForTopic(t)
		if err != nil {
			return err
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, len(partitions))
		for i, p := range partitions {
			partitionMetadata[i] = &protocol.PartitionMetadata{
				ParititionID: p.Partition,
			}
		}
		topics[i] = &protocol.TopicMetadata{
			TopicErrorCode:    protocol.ErrNone,
			Topic:             t,
			PartitionMetadata: partitionMetadata,
		}
	}
	resp := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topics,
	}
	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	b, err := protocol.Encode(r)
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

func (s *Server) handleProduce(conn net.Conn, header *protocol.RequestHeader, r *protocol.ProduceRequest) error {
	resp := new(protocol.ProduceResponses)
	resp.Responses = make([]*protocol.ProduceResponse, len(r.TopicData))
	for i, td := range r.TopicData {
		presps := make([]*protocol.ProducePartitionresponse, len(td.Data))
		for j, p := range td.Data {
			partition := &cluster.TopicPartition{
				Topic:     td.Topic,
				Partition: p.Partition,
			}
			presp := &protocol.ProducePartitionresponse{}
			partition, err := s.broker.Partition(td.Topic, p.Partition)
			if err != nil {
				presp.ErrorCode = protocol.ErrUnknownTopicOrPartition
			}
			if !s.broker.IsLeaderOfPartition(partition) {
				presp.ErrorCode = protocol.ErrNotLeaderForPartition
			}
			err = partition.CommitLog.Append(p.RecordSet)
			if err != nil {
				s.logger.Printf("commitlog append failed: %s", err)
				presp.ErrorCode = protocol.ErrUnknown
			}
			presp.Partition = p.Partition
			// TODO: presp.BaseOffset
			presp.Timestamp = time.Now().Unix()
			presps[j] = presp
		}
		resp.Responses[i] = &protocol.ProduceResponse{
			Topic:              td.Topic,
			PartitionResponses: presps,
		}
	}

	b, err := protocol.Encode(&protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

func (s *Server) handleFetch(conn net.Conn, header *protocol.RequestHeader, r *protocol.FetchRequest) error {
	resp := &protocol.FetchResponses{}
	resp.Responses = make([]*protocol.FetchResponse, len(r.Topics))
	received := time.Now()
	for i, topic := range r.Topics {
		fr := &protocol.FetchResponse{}
		resp.Responses[i] = fr
		fr.PartitionResponses = make([]*protocol.FetchPartitionResponse, len(topic.Partitions))
		for j, p := range topic.Partitions {
			partition, err := s.broker.Partition(topic.Topic, p.Partition)
			if err != nil {
				// TODO set err code
				s.logger.Printf("[ERR] jocko: Failed to find partition: %v (%s/%d)", err, topic.Topic, p.Partition)
				break
			}
			if !s.broker.IsLeaderOfPartition(partition) {
				s.logger.Printf("[ERR] jocko: Failed to produce: %v", errors.New("broker is not partition leader"))
				// TODO set err code
				break
			}
			rdr, err := partition.CommitLog.NewReader(commitlog.ReaderOptions{
				Offset:   p.FetchOffset,
				MaxBytes: p.MaxBytes,
			})
			if err != nil {
				s.logger.Printf("[ERR] jocko: Failed to read partition: %v", err)
				// TODO set err code
				break
			}
			b := bytes.NewBuffer(make([]byte, 0))
			var n int32
			for n < r.MinBytes {
				if r.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > r.MaxWaitTime {
					break
				}

				// TODO: copy these bytes to outer bytes
				nn, err := io.Copy(b, rdr)
				if err != nil && err != io.EOF {
					s.logger.Printf("[ERR] jocko: Failed to fetch messages: %v", err)
					// TODO seT error code
					break
				}
				n += int32(nn)
				if err == io.EOF {
					break
				}
			}
			fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
				Partition:     p.Partition,
				ErrorCode:     protocol.ErrNone,
				HighWatermark: 0, // TODO get last committed offset
				RecordSet:     b.Bytes(),
			}
		}
	}

	b, err := protocol.Encode(&protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}
