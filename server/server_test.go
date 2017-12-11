package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/jocko/zap"
)

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(1)
)

type config struct {
	id         int32
	serverPort string
	serfPort   string
	conn       *net.TCPConn
	broker     *broker.Broker
	shutdown   func()
}

func TestBroker(t *testing.T) {
	confs := setup(t, 3)
	defer func() {
		for i := range confs {
			defer confs[i].shutdown()
		}
	}()

	time.Sleep(3 * time.Second)

	for id, conf := range confs {
		if id == 0 {
			continue
		}
		if err := conf.broker.Join("127.0.0.1:" + confs[0].serfPort); err != protocol.ErrNone {
			t.Fatalf("err: %v", err)
		}
	}

	time.Sleep(3 * time.Second)

	client := server.NewClient(confs[0].conn)
	_, err := client.CreateTopics("testclient", &protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     int32(1),
			ReplicationFactor: int16(3),
			ReplicaAssignment: map[int32][]int32{
				0: []int32{0, 1, 2},
			},
			Configs: map[string]string{
				"config_key": "config_val",
			},
		}},
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	fmt.Println("i'm done")

	t.Run("Replication", func(tt *testing.T) {
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_0_0
		config.ChannelBufferSize = 1
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 10

		var brokers []string
		for _, conf := range confs {
			brokers = append(brokers, "127.0.0.1:"+conf.serverPort)
		}

		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			panic(err)
		}

		bValue := []byte("Hello from Jocko!")
		msgValue := sarama.ByteEncoder(bValue)
		pPartition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: msgValue,
		})
		fmt.Println(err)
		require.NoError(t, err)

		consumer, err := sarama.NewConsumer(brokers, config)
		require.NoError(t, err)

		cPartition, err := consumer.ConsumePartition(topic, pPartition, 0)
		require.NoError(t, err)

		select {
		case msg := <-cPartition.Messages():
			require.Equal(t, msg.Offset, offset)
			require.Equal(t, pPartition, msg.Partition)
			require.Equal(t, topic, msg.Topic)
			require.Equal(t, 0, bytes.Compare(bValue, msg.Value))
		case err := <-cPartition.Errors():
			require.NoError(t, err)
		}

		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			panic(err)
		}

		leader, err := client.Leader(topic, pPartition)
		if err != nil {
			panic(err)
		}
		before := leader.ID()
		confs[before].shutdown()
		delete(confs, before)

		time.Sleep(10 * time.Second)

		if err = client.RefreshMetadata(topic); err != nil {
			t.Errorf("err: %v", err)
		}

		leader, err = client.Leader(topic, pPartition)
		if err != nil {
			panic(err)
		}
		after := leader.ID()

		fmt.Printf("before: %d, after: %d\n", before, after)

		if before == after {
			tt.Fatalf("expected leader before not to be leader after: before: %d, after: %d", before, after)
		}

		// now consume the msg which should be replicated

		brokers = nil
		for _, conf := range confs {
			brokers = append(brokers, "127.0.0.1:"+conf.serverPort)
		}

		consumer, err = sarama.NewConsumer(brokers, config)
		require.NoError(t, err)

		cPartition, err = consumer.ConsumePartition(topic, pPartition, 0)
		require.NoError(t, err)

		select {
		case msg := <-cPartition.Messages():
			fmt.Printf("msg: %v\n", msg)
			require.Equal(t, msg.Offset, offset)
			require.Equal(t, pPartition, msg.Partition)
			require.Equal(t, topic, msg.Topic)
			require.Equal(t, 0, bytes.Compare(bValue, msg.Value))
		case err := <-cPartition.Errors():
			fmt.Printf("err: %v\n", err)
			require.NoError(t, err)
		}
	})
}

func BenchmarkBroker(b *testing.B) {
	confs := setup(b, 1)
	defer confs[0].shutdown()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	brokers := []string{"127.0.0.1:" + confs[0].serverPort}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	bValue := []byte("Hello from Jocko!")
	msgValue := sarama.ByteEncoder(bValue)

	var msgCount int

	b.Run("Sarama_Produce", func(b *testing.B) {
		msgCount = b.N

		for i := 0; i < b.N; i++ {
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: msgValue,
			})
			require.NoError(b, err)
		}
	})

	b.Run("Sarama_Consume", func(b *testing.B) {
		consumer, err := sarama.NewConsumer(brokers, config)
		require.NoError(b, err)

		cPartition, err := consumer.ConsumePartition(topic, 0, 0)
		require.NoError(b, err)

		for i := 0; i < msgCount; i++ {
			select {
			case msg := <-cPartition.Messages():
				require.Equal(b, topic, msg.Topic)
			case err := <-cPartition.Errors():
				require.NoError(b, err)
			}
		}
	})
}

func setup(t require.TestingT, brokers int) map[int32]config {
	confs := make(map[int32]config)
	for i := 0; i < brokers; i++ {
		dataDir, err := ioutil.TempDir("", "server_test_"+strconv.Itoa(i))
		require.NoError(t, err)

		ports := dynaport.GetS(4)
		serverPort := ports[0]
		serfPort := ports[2]

		logger := zap.New()
		serf, err := serf.New(
			serf.Logger(logger),
			serf.Addr("127.0.0.1:"+serfPort),
		)
		raft, err := raft.New(
			raft.Logger(logger),
			raft.DataDir(dataDir),
			raft.Addr("127.0.0.1:"+ports[1]),
		)
		id := int32(i)
		store, err := broker.New(id,
			broker.LogDir(dataDir),
			broker.Addr("127.0.0.1:"+serverPort),
			broker.Raft(raft),
			broker.Serf(serf),
			broker.Logger(logger),
		)
		require.NoError(t, err)

		_, err = store.WaitForLeader(10 * time.Second)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel((context.Background()))
		srv := server.New(":"+serverPort, store, ":"+ports[3], mock.NewMetrics(), logger)
		require.NotNil(t, srv)
		require.NoError(t, srv.Start(ctx))

		tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+serverPort)
		require.NoError(t, err)

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		require.NoError(t, err)

		confs[id] = config{
			int32(i), serverPort, serfPort, conn, store, func() {
				cancel()
				conn.Close()
				srv.Close()
				store.Shutdown()
				os.RemoveAll(dataDir)
			},
		}
	}
	return confs
}
