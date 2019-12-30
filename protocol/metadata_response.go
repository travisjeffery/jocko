package protocol

type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	// unsupported: Rack *string
}

type PartitionMetadata struct {
	PartitionErrorCode int16
	PartitionID        int32
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
	APIVersion int16

	Brokers       []*Broker
	ControllerID  int32
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
		e.PutString("")
	}
	if r.APIVersion >= 1 {
		e.PutInt32(r.ControllerID)
	}
	if err = e.PutArrayLength(len(r.TopicMetadata)); err != nil {
		return err
	}
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutInt8(0)
		if err = e.PutArrayLength(len(t.PartitionMetadata)); err != nil {
			return err
		}
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.PartitionID)
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

func (r *MetadataResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	brokerCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
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
	if version >= 1 {
		r.ControllerID, err = d.Int32()
		if err != nil {
			return err
		}
	}
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
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
			p.PartitionID, err = d.Int32()
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

func (r *MetadataResponse) Version() int16 {
	return r.APIVersion
}

type Brokers []*Broker

type TopicMetadatas []*TopicMetadata

type PartitionMetadatas []*PartitionMetadata
