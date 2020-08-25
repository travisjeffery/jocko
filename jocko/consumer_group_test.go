package jocko_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/config"
)

//modified from https://godoc.org/github.com/Shopify/sarama#example-ConsumerGroup
func beginConsume(consumer *Consumer, brokers string, group string, topics string) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	//config.Consumer.Group.Heartbeat.Interval = 300 * time.Millisecond
	//config.Consumer.Group.Session.Timeout = 1000 * time.Millisecond

	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	/**
	 * Setup a new Sarama consumer group
	 */

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	go func() {
		<-consumer.ready // Await till the consumer has been set up
		log.Println("Sarama consumer up and running!...")
		var shouldClose bool //if shouldclose, actively leave group
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
		case shouldClose = <-consumer.QuitChan:
			log.Println("quit chan")
		}
		cancel()
		wg.Wait()
		if shouldClose {
			if err = client.Close(); err != nil {
				log.Panicf("Error closing client: %v", err)
			}
		}
		log.Println("closed consumer group", consumer.ID)
	}()
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ID             int
	ready          chan bool
	ClaimChan      chan idAndClaim
	currentSession sarama.ConsumerGroupSession
	mu             *sync.Mutex
	MsgChan        chan *sarama.ConsumerMessage
	QuitChan       chan bool
}
type idAndClaim struct {
	ID    int
	claim sarama.ConsumerGroupClaim

	generation int32
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	consumer.ClaimChan <- idAndClaim{claim: claim, ID: consumer.ID, generation: session.GenerationID()}
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("consumer id %d, Message claimed: value = %s, timestamp = %v, topic = %s partition: %d, claim partition %d", consumer.ID, string(message.Value), message.Timestamp, message.Topic, message.Partition, claim.Partition())
		if claim.Partition() != message.Partition {
			panic("claim partition does not match message partition " + strconv.Itoa(int(claim.Partition())) + "," + strconv.Itoa(int(message.Partition)))
		}
		consumer.MsgChan <- message
	}

	return nil
}
func setupJocko(t *testing.T) (string, func([]string)) {
	s1, dir1 := jocko.NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
	}, nil)
	ctx1, cancel1 := context.WithCancel((context.Background()))
	err := s1.Start(ctx1)
	require.NoError(t, err)
	//jocko.WaitForLeader(t, s1)
	return s1.Addr().String(), func([]string) {
		cancel1()
		os.RemoveAll(dir1)
		s1.Shutdown()
	}
}
func setupKafka(kafkaAddr string) (string, func([]string)) {
	return kafkaAddr, func(topics []string) {
		b := sarama.NewBroker(kafkaAddr)
		config := sarama.NewConfig()
		config.Version = sarama.V0_10_2_0
		err := b.Open(config)
		if err != nil {
			panic(err)
		}
		_, err = b.DeleteTopics(&sarama.DeleteTopicsRequest{
			Topics: topics,
		})
	}
}
func createTopicSarama(t *testing.T, addr string, topicName string, partitionNum int32) {
	b := sarama.NewBroker(addr)
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	err := b.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.CreateTopics(&sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topicName: {
				NumPartitions: partitionNum,

				ReplicationFactor: 1,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}
func TestConsumerGroupSarama(t *testing.T) {
	JoinAndLeave(t, 10, 3)
	JoinAndLeave(t, 10, 4)
	JoinAndLeave(t, 10, 5)
}

func JoinAndLeave(t *testing.T, partitionNum, consumerNum int32) {
	var brokers string
	var brokerCleanup func([]string)
	if kafkaAddr := os.Getenv("KAFKA_ADDR"); len(kafkaAddr) > 0 {
		log.Println("KAFKA_ADDR:", kafkaAddr)
		brokers, brokerCleanup = setupKafka(kafkaAddr)
	} else {
		brokers, brokerCleanup = setupJocko(t)
	}
	time.Sleep(3 * time.Second)
	consumers := make([]*Consumer, consumerNum)

	topics := "test-parti"
	group := "testgroup"
	defer brokerCleanup(strings.Split(topics, ","))

	createTopicSarama(t, brokers, topics, partitionNum)

	for i := 0; i < len(consumers); i++ {
		consumers[i] = &Consumer{
			ID:        i,
			ready:     make(chan bool),
			mu:        &sync.Mutex{},
			ClaimChan: make(chan idAndClaim, partitionNum),
			MsgChan:   make(chan *sarama.ConsumerMessage, partitionNum),
			QuitChan:  make(chan bool),
		}
	}
	claimAggregate := make(chan idAndClaim, 2*partitionNum)
	for _, consumer := range consumers {
		go func(id int, c chan idAndClaim) {
			for claim := range c {
				claimAggregate <- claim
			}
		}(consumer.ID, consumer.ClaimChan)
	}
	for i := 0; i < len(consumers); i++ {
		go func(j int) {
			beginConsume(consumers[j], brokers, group, topics)
		}(i)
	}
	time.Sleep(10 * time.Second)
	checkRangeAssignmentClaims(t, claimAggregate, partitionNum, consumerNum)
	consumers[0].QuitChan <- true //actively send leave group request
	time.Sleep(10 * time.Second)
	checkRangeAssignmentClaims(t, claimAggregate, partitionNum, consumerNum-1)
	consumers[1].QuitChan <- false //without leave group, coordinator will find this consumer heartbeat timed out and rebalance
	time.Sleep(20 * time.Second)
	checkRangeAssignmentClaims(t, claimAggregate, partitionNum, consumerNum-2)
}
func checkRangeAssignmentClaims(t *testing.T, claimAggregate chan idAndClaim, partitionNum int32, consumerNum int32) {
	lastGenerationClaims, lastGeneration := getLastGeneration(claimAggregate)
	partitionIDtoConsumerID := make(map[int32]int)
	for _, claim := range lastGenerationClaims {
		partitionIDtoConsumerID[claim.claim.Partition()] = claim.ID
	}
	if int(partitionNum) != len(partitionIDtoConsumerID) {
		panic(fmt.Sprintf(
			"claims does not contains all partitions "+
				"partitionNum %d consumerNum %d  map: %v",
			partitionNum, consumerNum, partitionIDtoConsumerID))
	}

	consumerIDtoPartionID := make(map[int]map[int32]bool)
	for _, claim := range lastGenerationClaims {
		var m map[int32]bool
		var ok bool
		if m, ok = consumerIDtoPartionID[claim.ID]; !ok {
			m = make(map[int32]bool)
			consumerIDtoPartionID[claim.ID] = m
		}
		m[claim.claim.Partition()] = true
	}
	q := int(partitionNum / consumerNum)
	r := int(partitionNum % consumerNum)
	// under range assignment
	// r consumers each got q+1 partitions
	// consumerNum-r consumers each got q partition
	numQ := 0
	numQplus1 := 0
	for _, m := range consumerIDtoPartionID {
		switch len(m) {
		case q:
			numQ++
		case q + 1:
			numQplus1++
		}
	}
	isEven := r == numQplus1 && int(consumerNum)-r == numQ
	log.Printf("assignments %v partitionNum:%d consumerNum: %d  q:%d r:%d gen: %d %v",
		consumerIDtoPartionID, partitionNum, consumerNum, q, r, lastGeneration, isEven)
	if !isEven {
		panic("partition not even")
	}
}
func getLastGeneration(claimAggregate chan idAndClaim) ([]idAndClaim, int32) {
	var claims []idAndClaim
	//read until blocked
	for {
		blocked := false
		select {
		case msg := <-claimAggregate:
			claims = append(claims, msg)
		default:
			blocked = true
			break
		}
		if blocked {
			break
		}
	}
	log.Printf("%v len total claims %d", time.Now(), len(claims))
	//there may be a few rounds of rebalancing, so we only checks the last generation
	lastGeneration := int32(0)
	for _, claim := range claims {
		if claim.generation > lastGeneration {
			lastGeneration = claim.generation
		}
	}
	var lastGenerationClaims []idAndClaim
	for _, claim := range claims {
		if claim.generation == lastGeneration {
			lastGenerationClaims = append(lastGenerationClaims, claim)
		}
	}
	return lastGenerationClaims, lastGeneration
}
