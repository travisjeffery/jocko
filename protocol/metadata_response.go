package protocol

type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	// unsupported: Rack *string
}

type PartitionMetadata struct {
	PartitionErrorCode int16
	ParititionID       int32
	Leader             int32
	Replicas           []int32
	ISR                []int32
}

type TopicMetadata struct {
	TopicErrorCode    int16
	Topic             string
	PartitionMetadata []*PartitionMetadata
}

type MetadataResponse struct {
	Brokers []*Broker
	// unsupported: ClusterID *string
	// unsupported: ControllerID string
	TopicMetadata []*TopicMetadata
}

func (r *MetadataResponse) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Brokers)); err != nil {
		return err
	}
	for _, b := range r.Brokers {
		e.PutInt32(b.NodeID)
		if err = e.PutString(b.Host); err != nil {
			return err
		}
		e.PutInt32(b.Port)
	}
	if err = e.PutArrayLength(len(r.TopicMetadata)); err != nil {
		return err
	}
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(t.PartitionMetadata)); err != nil {
			return err
		}
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.ParititionID)
			e.PutInt32(p.Leader)
			if err = e.PutInt32Array(p.Replicas); err != nil {
				return err
			}
			if err = e.PutInt32Array(p.ISR); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *MetadataResponse) Decode(d PacketDecoder) error {
	brokerCount, err := d.ArrayLength()
	r.Brokers = make([]*Broker, brokerCount)
	for i := range r.Brokers {
		nodeID, err := d.Int32()
		if err != nil {
			return err
		}
		host, err := d.String()
		if err != nil {
			return err
		}
		port, err := d.Int32()
		if err != nil {
			return err
		}
		r.Brokers[i] = &Broker{
			NodeID: nodeID,
			Host:   host,
			Port:   port,
		}
	}
	topicCount, err := d.ArrayLength()
	r.TopicMetadata = make([]*TopicMetadata, topicCount)
	for i := range r.TopicMetadata {
		m := &TopicMetadata{}
		m.TopicErrorCode, err = d.Int16()
		if err != nil {
			return err
		}
		m.Topic, err = d.String()
		if err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		partitions := make([]*PartitionMetadata, partitionCount)
		for i := range partitions {
			p := &PartitionMetadata{}
			p.PartitionErrorCode, err = d.Int16()
			if err != nil {
				return err
			}
			p.ParititionID, err = d.Int32()
			if err != nil {
				return err
			}
			p.Leader, err = d.Int32()
			if err != nil {
				return err
			}
			p.Replicas, err = d.Int32Array()
			if err != nil {
				return err
			}
			p.ISR, err = d.Int32Array()
			partitions[i] = p
		}
		m.PartitionMetadata = partitions
		r.TopicMetadata[i] = m
	}
	return nil
}
