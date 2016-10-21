package cluster

import (
	"errors"

	"github.com/travisjeffery/jocko/store"
)

type Controller struct {
	store *store.Store
}

func (c *Controller) CreateTopic(topic string) error {
	for _, t := range c.store.Topics() {
		if t == topic {
			return errors.New("topic exists already")
		}
	}

	numPartitions, err := c.store.NumPartitions()
	if err != nil {
		return err
	}
	brokers, err := c.store.Brokers()
	if err != nil {
		return err
	}

	for i := 0; i < numPartitions; i++ {
		broker := brokers[len(brokers)%i]
		partition := store.TopicPartition{
			Topic:           topic,
			Leader:          broker,
			PreferredLeader: broker,
			Replicas:        []string{broker},
		}
		if err := c.store.AddPartition(partition); err != nil {
			return err
		}
	}
}
