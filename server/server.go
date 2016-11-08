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
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
)

type MetadataRequest struct {
	Topics []string `json:"topics"`
}

type Broker struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port string `json:"port"`
}

type PartitionMetadata struct {
	ErrorCode int      `json:"error_code"`
	ID        int32    `json:"id"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
}

type TopicMetadata struct {
	ErrorCode         int                 `json:"error_code"`
	Topic             string              `json:"topic"`
	PartitionMetadata []PartitionMetadata `json:"partition_metadata"`
}

type MetadataResponse struct {
	Brokers       []Broker        `json:"brokers"`
	ControllerID  string          `json:"controller_id"`
	TopicMetadata []TopicMetadata `json:"topic_metadata"`
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

	// r.Methods("POST").Path("/metadata").HandlerFunc(s.handleMetadata)
	// r.Methods("POST").Path("/metadata/topic").HandlerFunc(s.handleTopic)
	// 	r.Methods("POST").Path("/produce").HandlerFunc(s.handleProduce)
	// r.Methods("POST").Path("/fetch").HandlerFunc(s.handleFetch)
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
			s.logger.Printf("conn read EOF: %s", err)
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

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

type TopicRequest struct {
	Topic      string `json:"topic"`
	Partitions int    `json"partitions"`
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	// var topic TopicRequest
	// if err := json.NewDecoder(r.Body).Decode(&topic); err != nil {
	// 	s.logger.Printf("[ERR] jocko: Failed to decode json; %v", errors.Wrap(err, "json decode failed"))
	// 	w.WriteHeader(http.StatusBadRequest)
	// 	return
	// }
	// if topic.Topic == "" {
	// 	http.Error(w, "topic is blank", http.StatusBadRequest)
	// 	return
	// }
	// if topic.Partitions <= 0 {
	// 	http.Error(w, "partitions is 0", http.StatusBadRequest)
	// 	return
	// }
	// if s.broker.IsController() {
	// 	err := s.broker.CreateTopic(topic.Topic, topic.Partitions)
	// 	if err != nil {
	// 		s.logger.Printf("[ERR] jocko: Failed to create topic %s: %v", topic.Topic, err)
	// 		w.WriteHeader(http.StatusInternalServerError)
	// 		return
	// 	}
	// } else {
	// 	cID := s.broker.ControllerID()
	// 	http.Redirect(w, r, cID, http.StatusSeeOther)
	// 	return
	// }
	// w.WriteHeader(http.StatusOK)
}

type ProduceRequest struct {
	RequiredAcks int                  `json:"required_acks"`
	Timeout      int                  `json:"timeout"`
	Partition    int32                `json:"partition"`
	Topic        string               `json:"topic"`
	MessageSet   commitlog.MessageSet `json:"message_set"`
}

type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	ErrorCode int16  `json:"error_code"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	var produce ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&produce); err != nil {
		s.logger.Printf("[ERR] jocko: Failed to decode json; %v", errors.Wrap(err, "json decode failed"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	partition, err := s.broker.Partition(produce.Topic, produce.Partition)
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to find partition: %v (%s/%d)", err, produce.Topic, produce.Partition)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !s.broker.IsLeaderOfPartition(partition) {
		s.logger.Printf("[ERR] jocko: Failed to produce: %v", errors.New("broker is not partition leader"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = partition.CommitLog.Append(produce.MessageSet)
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to append messages: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSON(w, ProduceResponse{
		Topic:     produce.Topic,
		Partition: produce.Partition,
	}, http.StatusOK)
}

type FetchRequest struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	FetchOffset int64  `json:"offset"`
	MinBytes    int32  `json:"min_bytes"`
	MaxBytes    int32  `json:"max_bytes"`
	MaxWaitTime int32  `json:"max_wait_time"` // in ms
}

type FetchResponse struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// b in bytes
	MessageSetSize int32                `json:"message_set_size"`
	MessageSet     commitlog.MessageSet `json:"message_set"`
}

func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	var fetch FetchRequest
	received := time.Now()
	if err := json.NewDecoder(r.Body).Decode(&fetch); err != nil {
		s.logger.Printf("[ERR] jocko: Failed to decode json; %v", errors.Wrap(err, "json decode failed"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	partition, err := s.broker.Partition(fetch.Topic, fetch.Partition)
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to find partition: %v (%s/%d)", err, fetch.Topic, fetch.Partition)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !s.broker.IsLeaderOfPartition(partition) {
		s.logger.Printf("[ERR] jocko: Failed to produce: %v", errors.New("broker is not partition leader"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rdr, err := partition.CommitLog.NewReader(commitlog.ReaderOptions{
		Offset:   fetch.FetchOffset,
		MaxBytes: fetch.MaxBytes,
	})
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to read partition: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	p := bytes.NewBuffer(make([]byte, 0))
	var n int32
	for n < fetch.MinBytes {
		if fetch.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > fetch.MaxWaitTime {
			break
		}

		// TODO: copy these bytes to outer bytes
		nn, err := io.Copy(p, rdr)
		if err != nil && err != io.EOF {
			s.logger.Printf("[ERR] jocko: Failed to fetch messages: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		n += int32(nn)
		if err == io.EOF {
			break
		}
	}
	v := FetchResponse{
		Topic:          fetch.Topic,
		Partition:      fetch.Partition,
		MessageSetSize: n,
		MessageSet:     p.Bytes(),
	}
	writeJSON(w, v, http.StatusOK)
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

func writeJSON(w http.ResponseWriter, v interface{}, code ...int) {
	var b []byte
	var err error
	b, err = json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if len(code) > 0 {
		w.WriteHeader(code[0])
	}
	w.Write(b)
}
