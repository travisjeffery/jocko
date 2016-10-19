package store

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStoreOpen(t *testing.T) {
	dataDir, _ := ioutil.TempDir("", "storetest")
	defer os.RemoveAll(dataDir)
	bindAddr := "127.0.0.1:0"

	s := New(dataDir, bindAddr)
	assert.NotNil(t, s)
	assert.NoError(t, s.Open())
}
