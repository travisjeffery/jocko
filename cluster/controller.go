package cluster

import (
	"errors"

	"github.com/travisjeffery/jocko/store"
)

type Controller struct {
	Store *store.Store
}

func (c *Controller) CreateTopic(topic string, partitions int) error {
	for _, t := range c.Store.Topics() {
		if t == topic {
			return errors.New("topic exists already")
		}
	}
	numPartitions, err := c.Store.NumPartitions()
	if err != nil {
		return err
	}
	if partitions != 0 {
		numPartitions = partitions
	}
	brokers, err := c.Store.Brokers()
	if err != nil {
		return err
	}
	for i := 0; i < numPartitions; i++ {
		broker := brokers[i%len(brokers)]
		partition := store.TopicPartition{
			Partition:       i,
			Topic:           topic,
			Leader:          broker,
			PreferredLeader: broker,
			Replicas:        []string{broker},
		}
		if err := c.Store.AddPartition(partition); err != nil {
			return err
		}
	}
	return nil
}
