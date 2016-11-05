package client

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/server"
)

type ClientTestSuite struct {
	suite.Suite

	broker *broker.Broker
	server *server.Server
}

func (suite *ClientTestSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "clientest")
	assert.NoError(suite.T(), err)
	defer os.RemoveAll(dir)
	logs := filepath.Join(dir, "logs")
	assert.NoError(suite.T(), os.MkdirAll(logs, 0755))

	data := filepath.Join(dir, "data")
	assert.NoError(suite.T(), os.MkdirAll(data, 0755))

	suite.broker = broker.New(broker.Options{
		DataDir:  data,
		BindAddr: ":0",
		LogDir:   logs,
	})
	assert.NoError(suite.T(), suite.broker.Open())

	_, err = suite.broker.WaitForLeader(10 * time.Second)
	assert.NoError(suite.T(), err)

	err = suite.broker.CreateTopic("my_topic", 2)
	assert.NoError(suite.T(), err)

	suite.server = server.New("localhost:3000", suite.broker)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), suite.server)
	assert.NoError(suite.T(), suite.server.Start())
}

func (suite *ClientTestSuite) TeardownSuite() {
	suite.broker.Close()
	suite.server.Close()
}

func (suite *ClientTestSuite) TestRefreshMetadata() {
	c := New(Options{
		Addr: "http://localhost:3000",
	})
	metadata, err := c.Metadata()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ":0", metadata.ControllerID)
	assert.Equal(suite.T(), ":0", metadata.Brokers[0].ID)
	assert.Equal(suite.T(), "my_topic", metadata.TopicMetadata[0].Topic)
	assert.Equal(suite.T(), ":0", metadata.TopicMetadata[0].PartitionMetadata[0].Leader)
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}
