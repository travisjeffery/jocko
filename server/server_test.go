package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/store"
)

func TestNewServer(t *testing.T) {
	dir := os.TempDir()
	os.RemoveAll(dir)

	logs := filepath.Join(dir, "logs")
	assert.NoError(t, os.MkdirAll(logs, 0755))

	data := filepath.Join(dir, "data")
	assert.NoError(t, os.MkdirAll(data, 0755))

	store := store.New(store.Options{
		DataDir:  data,
		BindAddr: ":0",
		LogDir:   logs,
	})
	assert.NoError(t, store.Open())
	defer store.Close()

	_, err := store.WaitForLeader(10 * time.Second)
	assert.NoError(t, err)

	s := New(":0", store)
	assert.NotNil(t, s)
	assert.NoError(t, s.Start())

	ms := commitlog.NewMessageSet([]commitlog.Message{commitlog.NewMessage([]byte("Hello, World"))}, 1)
	mse := base64.StdEncoding.EncodeToString(ms)

	topic := http.HandlerFunc(s.handleTopic)
	rr := httptest.NewRecorder()
	r := bytes.NewReader([]byte(`{"partitions": 2, "topic": "my_topic"}`))
	req, err := http.NewRequest("POST", "/metadata/topic", r)
	assert.NoError(t, err)
	topic.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	produce := http.HandlerFunc(s.handleProduce)
	rr = httptest.NewRecorder()
	j := fmt.Sprintf(`{"partition": 0, "topic": "my_topic", "message_set": "%s"}`, mse)
	r = bytes.NewReader([]byte(j))
	req, err = http.NewRequest("POST", "/produce", r)
	assert.NoError(t, err)
	produce.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	fetch := http.HandlerFunc(s.handleFetch)
	rr = httptest.NewRecorder()
	r = bytes.NewReader([]byte(`{"topic": "my_topic", "partition": 0, "offset": 1, "min_bytes": 100, "max_bytes": 200}`))
	req, err = http.NewRequest("POST", "/fetch", r)
	assert.NoError(t, err)
	fetch.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	resp := new(FetchResponse)
	assert.NoError(t, json.Unmarshal(rr.Body.Bytes(), resp))
	assert.Equal(t, "my_topic", resp.Topic)
	assert.Equal(t, 0, resp.Partition)
	fmt.Println(resp.MessageSet)
	assert.True(t, bytes.Compare(ms, resp.MessageSet) == 0)
}
