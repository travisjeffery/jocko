package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewServer(t *testing.T) {
	store := newTestStore()
	s := &testServer{New(":0", store)}

	assert.NotNil(t, s)
	assert.NoError(t, s.Start())

	b := doGet(t, s.URL(), "k1")
	assert.Equal(t, `{"k1":""}`, string(b))

	doPost(t, s.URL(), "k1", "v1")
	b = doGet(t, s.URL(), "k1")
	assert.Equal(t, `{"k1":"v1"}`, string(b))

	store.m["k2"] = []byte("v2")
	b = doGet(t, s.URL(), "k2")
	assert.Equal(t, `{"k2":"v2"}`, string(b))

	doDelete(t, s.URL(), "k2")
	b = doGet(t, s.URL(), "k2")
	assert.Equal(t, `{"k2":""}`, string(b))
}

type testServer struct {
	*Server
}

func (t *testServer) URL() string {
	port := strings.TrimLeft(t.Addr().String(), "[::]:")
	return fmt.Sprintf("http://127.0.0.1:%s", port)
}

type testStore struct {
	m map[string][]byte
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string][]byte),
	}
}

func (t *testStore) Get(key []byte) ([]byte, error) {
	return t.m[string(key)], nil
}

func (t *testStore) Set(key, value []byte) error {
	t.m[string(key)] = value
	return nil
}

func (t *testStore) Delete(key []byte) error {
	delete(t.m, string(key))
	return nil
}

func (t *testStore) Join(addr []byte) error {
	return nil
}

func doGet(t *testing.T, url, key string) string {
	resp, err := http.Get(fmt.Sprintf("%s/key/%s", url, key))
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	return string(body)
}

func doPost(t *testing.T, url, key, value string) {
	b, err := json.Marshal(map[string]string{key: value})
	assert.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("%s/key", url), "application-type/json", bytes.NewReader(b))
	defer resp.Body.Close()
	assert.NoError(t, err)

}

func doDelete(t *testing.T, u, key string) {
	ru, err := url.Parse(fmt.Sprintf("%s/key/%s", u, key))
	assert.NoError(t, err)
	req := &http.Request{
		Method: "DELETE",
		URL:    ru,
	}
	client := http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	assert.NoError(t, err)
}
