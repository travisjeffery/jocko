package server

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/protocol"
)

func TestNewServer(t *testing.T) {
	dir := os.TempDir()
	os.RemoveAll(dir)

	logs := filepath.Join(dir, "logs")
	assert.NoError(t, os.MkdirAll(logs, 0755))

	data := filepath.Join(dir, "data")
	assert.NoError(t, os.MkdirAll(data, 0755))

	store := broker.New(broker.Options{
		DataDir:  data,
		BindAddr: ":0",
		LogDir:   logs,
	})
	assert.NoError(t, store.Open())
	defer store.Close()

	_, err := store.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	s := New(":8000", store)
	assert.NotNil(t, s)
	assert.NoError(t, s.Start())

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":8000")
	assert.NoError(t, err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	assert.NoError(t, err)
	defer conn.Close()

	body := &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             "test_topic",
			NumPartitions:     int32(8),
			ReplicationFactor: int16(2),
			ReplicaAssignment: map[int32][]int32{
				0: []int32{1, 2},
			},
			Configs: map[string]string{
				"config_key": "config_val",
			},
		},
		}}

	req := &protocol.Request{
		CorrelationID: int32(rand.Uint32()),
		ClientID:      "test_client",
		Body:          body,
	}

	b, err := protocol.Encode(req)
	assert.NoError(t, err)

	_, err = conn.Write(b)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, conn)
	assert.NoError(t, err)

	var resp protocol.CreateTopicsResponse
	err = protocol.Decode(buf.Bytes(), &resp)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(resp.TopicErrorCodes))
	assert.Equal(t, "test_topic", resp.TopicErrorCodes[0].Topic)
	assert.Equal(t, protocol.ErrNone, resp.TopicErrorCodes[0].ErrorCode)

	return

	// b := new(bytes.Buffer)
	// p := make([]byte, 4)
	// encoding.Enc.PutUint32(p, 0)
	// assert.NoError(t, err)
	// _, err = b.Write(p)
	// assert.NoError(t, err)

	// clientID := "test_client"
	// header := RequestHeader{
	// 	Size:          0,
	// 	APIKey:        0,
	// 	APIVersion:    2,
	// 	CorrelationID: int32(1),
	// 	ClientIDSize:  int16(len(clientID)),
	// }
	// err = encoding.Write(b, header)
	// assert.NoError(t, err)

	// lclientID := len(clientID)
	// size := encoding.Size(header)
	// size += lclientID
	// err = encoding.Write(b, int16(lclientID))
	// assert.NoError(t, err)

	// encoding.Enc.PutUint32(b.Bytes()[0:4], uint32(size))

	// _, err = b.WriteTo(conn)
	// assert.NoError(t, err)

	// ms := commitlog.NewMessageSet([]commitlog.Message{commitlog.NewMessage([]byte("Hello, World"))}, 1)
	// mse := base64.StdEncoding.EncodeToString(ms)

	// topic := http.HandlerFunc(s.handleTopic)
	// rr := httptest.NewRecorder()
	// r := bytes.NewReader([]byte(`{"partitions": 2, "topic": "my_topic"}`))
	// req, err := http.NewRequest("POST", "/metadata/topic", r)
	// assert.NoError(t, err)
	// topic.ServeHTTP(rr, req)
	// assert.Equal(t, http.StatusOK, rr.Code)

	// produce := http.HandlerFunc(s.handleProduce)
	// rr = httptest.NewRecorder()
	// j := fmt.Sprintf(`{"partition": 0, "topic": "my_topic", "message_set": "%s"}`, mse)
	// r = bytes.NewReader([]byte(j))
	// req, err = http.NewRequest("POST", "/produce", r)
	// assert.NoError(t, err)
	// produce.ServeHTTP(rr, req)
	// assert.Equal(t, http.StatusOK, rr.Code)

	// fetch := http.HandlerFunc(s.handleFetch)
	// rr = httptest.NewRecorder()
	// r = bytes.NewReader([]byte(`{"max_wait_time": 100, "topic": "my_topic", "partition": 0, "offset": 1, "min_bytes": 100, "max_bytes": 200}`))
	// req, err = http.NewRequest("POST", "/fetch", r)
	// assert.NoError(t, err)
	// fetch.ServeHTTP(rr, req)
	// assert.Equal(t, http.StatusOK, rr.Code)
	// resp := new(FetchResponse)
	// assert.NoError(t, json.Unmarshal(rr.Body.Bytes(), resp))
	// assert.Equal(t, "my_topic", resp.Topic)
	// assert.Equal(t, 0, resp.Partition)
	// assert.True(t, bytes.Compare(ms, resp.MessageSet) == 0)
}
