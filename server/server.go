package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/store"
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
	ID        int      `json:"id"`
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
	ln   net.Listener

	logger *log.Logger
	store  *store.Store
}

func New(addr string, store *store.Store) *Server {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	return &Server{
		addr:   addr,
		store:  store,
		logger: logger,
	}
}

// Start starts the service.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	r := mux.NewRouter()
	r.Methods("POST").Path("/metadata").HandlerFunc(s.handleMetadata)
	r.Methods("POST").Path("/metadata/topic").HandlerFunc(s.handleTopic)
	r.Methods("POST").Path("/join").HandlerFunc(s.handleJoin)
	r.Methods("POST").Path("/produce").HandlerFunc(s.handleProduce)
	r.Methods("POST").Path("/fetch").HandlerFunc(s.handleFetch)
	r.PathPrefix("").HandlerFunc(s.handleNotFound)
	http.Handle("/", r)

	loggedRouter := handlers.LoggingHandler(os.Stdout, r)

	server := http.Server{
		Handler: loggedRouter,
	}

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

	if err := s.store.Join(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	var m MetadataRequest
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		s.logger.Print(errors.Wrap(err, "json decode failed"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	brokerIDs, err := s.store.Brokers()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var brokers []Broker
	for _, bID := range brokerIDs {
		host, port, err := net.SplitHostPort(bID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		brokers = append(brokers, Broker{
			ID:   bID,
			Host: host,
			Port: port,
		})
	}
	topic := s.store.Topics()
	var topicMetadata []TopicMetadata
	for _, t := range topic {
		partitions, err := s.store.PartitionsForTopic(t)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var partitionMetadata []PartitionMetadata
		for _, p := range partitions {
			partitionMetadata = append(partitionMetadata, PartitionMetadata{
				ID:       p.Partition,
				Replicas: p.Replicas,
				Leader:   p.Leader,
			})
		}
		topicMetadata = append(topicMetadata, TopicMetadata{
			Topic:             t,
			PartitionMetadata: partitionMetadata,
		})
	}
	v := MetadataResponse{
		Brokers:       brokers,
		ControllerID:  s.store.ControllerID(),
		TopicMetadata: topicMetadata,
	}
	writeJSON(w, v)
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

type TopicRequest struct {
	Topic      string `json:"topic"`
	Partitions int    `json"partitions"`
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	var topic TopicRequest
	if err := json.NewDecoder(r.Body).Decode(&topic); err != nil {
		s.logger.Printf("[ERR] jocko: Failed to decode json; %v", errors.Wrap(err, "json decode failed"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if topic.Topic == "" {
		http.Error(w, "topic is blank", http.StatusBadRequest)
		return
	}
	if topic.Partitions <= 0 {
		http.Error(w, "partitions is 0", http.StatusBadRequest)
		return
	}
	if s.store.IsController() {
		err := s.store.CreateTopic(topic.Topic, topic.Partitions)
		if err != nil {
			s.logger.Printf("[ERR] jocko: Failed to create topic %s: %v", topic.Topic, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		cID := s.store.ControllerID()
		http.Redirect(w, r, cID, http.StatusSeeOther)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type ProduceRequest struct {
	RequiredAcks int                  `json:"required_acks"`
	Timeout      int                  `json:"timeout"`
	Partition    int                  `json:"partition"`
	Topic        string               `json:"topic"`
	MessageSet   commitlog.MessageSet `json:"message_set"`
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	var produce ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&produce); err != nil {
		s.logger.Printf("[ERR] jocko: Failed to decode json; %v", errors.Wrap(err, "json decode failed"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	partition, err := s.store.Partition(produce.Topic, produce.Partition)
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to find partition: %v (%s/%d)", err, produce.Topic, produce.Partition)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !s.store.IsLeaderOfPartition(partition) {
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
	w.WriteHeader(http.StatusOK)
}

type FetchRequest struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	FetchOffset int64  `json:"offset"`
	MinBytes    int32  `json:"min_bytes"`
	MaxBytes    int32  `json:"max_bytes"`
	MaxWaitTime int32  `json:"max_wait_time"` // in ms
}

type FetchResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	// size in bytes
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
	partition, err := s.store.Partition(fetch.Topic, fetch.Partition)
	if err != nil {
		s.logger.Printf("[ERR] jocko: Failed to find partition: %v (%s/%d)", err, fetch.Topic, fetch.Partition)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !s.store.IsLeaderOfPartition(partition) {
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
	p := make([]byte, fetch.MaxBytes)
	var n int32
	for n < fetch.MinBytes {
		fmt.Println(int32(time.Since(received).Nanoseconds() / 1e6))
		if fetch.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > fetch.MaxWaitTime {
			break
		}

		// TODO: copy these bytes to outer bytes
		nn, err := rdr.Read(p)
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
		MessageSet:     p[:n],
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
