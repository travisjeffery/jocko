package cluster

type BrokerOptions struct {
	ID string
}

type Broker struct {
	BrokerOptions
}

func NewBroker(opts BrokerOptions) *Broker {
	return &Broker{
		BrokerOptions: opts,
	}
}
