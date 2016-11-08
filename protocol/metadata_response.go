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

func (r *MetadataResponse) Encode(e PacketEncoder) error {
	e.PutArrayLength(len(r.Brokers))
	for _, b := range r.Brokers {
		e.PutInt32(b.NodeID)
		e.PutString(b.Host)
		e.PutInt32(b.Port)
	}
	e.PutArrayLength(len(r.TopicMetadata))
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		e.PutString(t.Topic)
		e.PutArrayLength(len(t.PartitionMetadata))
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.ParititionID)
			e.PutInt32(p.Leader)
			e.PutInt32Array(p.Replicas)
			e.PutInt32Array(p.ISR)
		}
	}
	return nil
}

func (r *MetadataResponse) Decode(d PacketDecoder) error {
	blen, err := d.ArrayLength()
	r.Brokers = make([]*Broker, blen)
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
	tlen, err := d.ArrayLength()
	r.TopicMetadata = make([]*TopicMetadata, tlen)
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
		plen, err := d.ArrayLength()
		if err != nil {
			return err
		}
		partitions := make([]*PartitionMetadata, plen)
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
