package server

import (
	"bytes"
	"encoding/json"
	"io"
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
	"github.com/travisjeffery/simplelog"
)

type Broker struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port string `json:"port"`
}

type Server struct {
	addr string
	ln   *net.TCPListener

	logger *simplelog.Logger
	broker *broker.Broker
}

func New(addr string, broker *broker.Broker, logger *simplelog.Logger) *Server {
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
		for {
			conn, err := s.ln.Accept()
			if err != nil {
				panic(errors.Wrap(err, "Listener accept failed"))
			}

			go s.handleRequest(conn)
		}
	}()

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			panic(errors.Wrap(err, "HTTP serve"))
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
			s.logger.Info("Read deadilne failed: %s", err)
			continue
		}
		n, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle err
			s.logger.Info("Conn read failed: %s", err)
			break
		}
		if n == 0 {
			continue
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			continue
		}

		s.logger.Debug("request with size: %d", size)
		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		_, err = io.ReadFull(conn, b[4:])
		if err != nil {
			panic(err)
		}

		d := protocol.NewDecoder(b)
		header.Decode(d)
		s.logger.Debug("correlation id [%d], request size [%d], key [%d]", header.CorrelationID, size, header.APIKey)

		switch header.APIKey {
		case 0:
			req := &protocol.ProduceRequest{}
			s.decode(header, req, d)
			if err = s.handleProduce(conn, header, req); err != nil {
				s.logger.Info("Produce failed: %s", err)
			}
		case 1:
			req := &protocol.FetchRequest{}
			s.decode(header, req, d)
			if err = s.handleFetch(conn, header, req); err != nil {
				s.logger.Info("Fetch failed: %s", err)
			}
		case 2:
			req := &protocol.OffsetsRequest{}
			s.decode(header, req, d)
			if err = s.handleOffsets(conn, header, req); err != nil {
				s.logger.Info("Offsets failed: %s", err)
			}
		case 3:
			req := &protocol.MetadataRequest{}
			s.decode(header, req, d)
			if err = s.handleMetadata(conn, header, req); err != nil {
				s.logger.Info("Metadata request failed: %s", err)
			}
		case 19:
			req := &protocol.CreateTopicRequests{}
			s.decode(header, req, d)
			if err = s.handleCreateTopic(conn, header, req); err != nil {
				s.logger.Info("Create topic failed: %s", err)
			}
		case 20:
			req := &protocol.DeleteTopicsRequest{}
			s.decode(header, req, d)
			if err = s.handleDeleteTopics(conn, header, req); err != nil {
				s.logger.Info("Delete topic failed: %s", err)
			}
		}
	}
}

func (s *Server) decode(header *protocol.RequestHeader, req protocol.Decoder, d protocol.PacketDecoder) error {
	err := req.Decode(d)
	if err != nil {
		return err
	}
	// s.logger.Debug("[%d], request: %s", header.CorrelationID, spew.Sdump(req))
	return nil
}

func (s *Server) handleCreateTopic(conn net.Conn, header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) (err error) {
	resp := new(protocol.CreateTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController, err := s.broker.IsController()
	if err != nil {
		return err
	}
	if isController {
		for i, req := range reqs.Requests {
			err = s.broker.CreateTopic(req.Topic, req.NumPartitions)
			if err != nil {
				s.logger.Info("Failed to create topic %s: %v", req.Topic, err)
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
	return s.write(conn, header, r)
}

func (s *Server) handleDeleteTopics(conn net.Conn, header *protocol.RequestHeader, reqs *protocol.DeleteTopicsRequest) (err error) {
	resp := new(protocol.DeleteTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Topics))
	isController, err := s.broker.IsController()
	if err != nil {
		return err
	}
	if isController {
		for i, topic := range reqs.Topics {
			err = s.broker.DeleteTopic(topic)
			if err != nil {
				s.logger.Info("Failed to delete topic %s: %v", topic, err)
				return
			}
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
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
	return s.write(conn, header, r)
}

func zero(p []byte) {
	for i := range p {
		p[i] = 0
	}
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	b := new(cluster.Broker)
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// TODO: change join to take a broker
	if err := s.broker.Join(b.ID, b.Host); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleMetadata(conn net.Conn, header *protocol.RequestHeader, req *protocol.MetadataRequest) error {
	brokers := make([]*protocol.Broker, len(s.broker.Brokers))
	topics := make([]*protocol.TopicMetadata, len(req.Topics))
	for i, b := range s.broker.Brokers {
		port, err := strconv.Atoi(b.Port)
		if err != nil {
			return err
		}
		brokers[i] = &protocol.Broker{
			NodeID: int32(b.ID),
			Host:   b.Host,
			Port:   int32(port),
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
	return s.write(conn, header, r)
}

func (s *Server) write(conn net.Conn, header *protocol.RequestHeader, e protocol.Encoder) error {
	// s.logger.Debug("correlation id [%d], response: %s", header.CorrelationID, spew.Sdump(e))
	b, err := protocol.Encode(e)
	if err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

func (s *Server) handleOffsets(conn net.Conn, header *protocol.RequestHeader, req *protocol.OffsetsRequest) error {
	oResp := new(protocol.OffsetsResponse)
	oResp.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		oResp.Responses[i] = new(protocol.OffsetResponse)
		oResp.Responses[i].Topic = t.Topic
		oResp.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, len(t.Partitions))
		for j, p := range t.Partitions {
			pResp := new(protocol.PartitionResponse)
			pResp.Partition = p.Partition

			partition, err := s.broker.Partition(t.Topic, p.Partition)

			var offset int64
			if err != nil {
				pResp.ErrorCode = protocol.ErrUnknown
				continue
			}
			if p.Timestamp == -2 {
				offset = partition.CommitLog.OldestOffset()
			} else {
				offset = partition.CommitLog.NewestOffset()
			}
			pResp.Offsets = []int64{offset}

			oResp.Responses[i].PartitionResponses[j] = pResp
		}
	}
	resp := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          oResp,
	}
	return s.write(conn, header, resp)
}

func (s *Server) handleProduce(conn net.Conn, header *protocol.RequestHeader, req *protocol.ProduceRequest) error {
	resp := new(protocol.ProduceResponses)
	resp.Responses = make([]*protocol.ProduceResponse, len(req.TopicData))
	for i, td := range req.TopicData {
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
			offset, err := partition.CommitLog.Append(p.RecordSet)
			if err != nil {
				s.logger.Info("commitlog append failed: %s", err)
				presp.ErrorCode = protocol.ErrUnknown
			}
			presp.Partition = p.Partition
			presp.BaseOffset = offset
			presp.Timestamp = time.Now().Unix()
			presps[j] = presp
		}
		resp.Responses[i] = &protocol.ProduceResponse{
			Topic:              td.Topic,
			PartitionResponses: presps,
		}
	}
	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	return s.write(conn, header, r)
}

func (s *Server) handleFetch(conn net.Conn, header *protocol.RequestHeader, r *protocol.FetchRequest) error {
	fresp := &protocol.FetchResponses{}
	fresp.Responses = make([]*protocol.FetchResponse, len(r.Topics))
	received := time.Now()
	for i, topic := range r.Topics {
		fr := &protocol.FetchResponse{}
		fresp.Responses[i] = fr
		fr.Topic = topic.Topic
		fr.PartitionResponses = make([]*protocol.FetchPartitionResponse, len(topic.Partitions))
		for j, p := range topic.Partitions {
			partition, err := s.broker.Partition(topic.Topic, p.Partition)
			if err != nil {
				// TODO set err code
				s.logger.Info("Failed to find partition: %v (%s/%d)", err, topic.Topic, p.Partition)
				break
			}
			if !s.broker.IsLeaderOfPartition(partition) {
				s.logger.Info("Failed to produce: %v", errors.New("broker is not partition leader"))
				// TODO set err code
				break
			}
			rdr, err := partition.CommitLog.NewReader(commitlog.ReaderOptions{
				Offset:   p.FetchOffset,
				MaxBytes: p.MaxBytes,
			})
			if err != nil {
				s.logger.Info("Failed to read partition: %v", err)
				// TODO set err code
				break
			}
			b := new(bytes.Buffer)
			var n int32
			for n < r.MinBytes {
				if r.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > r.MaxWaitTime {
					break
				}
				// TODO: copy these bytes to outer bytes
				nn, err := io.Copy(b, rdr)
				if err != nil && err != io.EOF {
					s.logger.Info("Failed to fetch messages: %v", err)
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
				HighWatermark: partition.CommitLog.NewestOffset(),
				RecordSet:     b.Bytes(),
			}
		}
	}
	resp := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          fresp,
	}
	return s.write(conn, header, resp)
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}
