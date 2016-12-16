package protocol

type CreateTopicRequest struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	Configs           map[string]string
}

type CreateTopicRequests struct {
	Requests []*CreateTopicRequest
	Timeout  int32
}

func (c *CreateTopicRequests) Encode(e PacketEncoder) error {
	e.PutArrayLength(len(c.Requests))
	for _, r := range c.Requests {
		e.PutString(r.Topic)
		e.PutInt32(r.NumPartitions)
		e.PutInt16(r.ReplicationFactor)
		e.PutArrayLength(len(r.ReplicaAssignment))
		for pid, ass := range r.ReplicaAssignment {
			e.PutInt32(pid)
			for _, a := range ass {
				e.PutInt32(a)
			}
		}
		e.PutArrayLength(len(r.Configs))
		for k, v := range r.Configs {
			e.PutString(k)
			e.PutString(v)
		}
	}
	e.PutInt32(c.Timeout)
	return nil
}

func (c *CreateTopicRequests) Decode(d PacketDecoder) error {
	var err error
	requestCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	c.Requests = make([]*CreateTopicRequest, requestCount)
	for i := range c.Requests {
		req := new(CreateTopicRequest)
		c.Requests[i] = req

		req.Topic, err = d.String()
		if err != nil {
			return err
		}
		req.NumPartitions, err = d.Int32()
		if err != nil {
			return err
		}
		req.ReplicationFactor, err = d.Int16()
		if err != nil {
			return err
		}
		assignmentCount, err := d.ArrayLength()
		ra := make(map[int32][]int32, assignmentCount)
		for i := 0; i < assignmentCount; i++ {
			pid, err := d.Int32()
			if err != nil {
				return err
			}
			replicaCount, err := d.ArrayLength()
			if err != nil {
				return err
			}
			reps := make([]int32, replicaCount)
			for i := range reps {
				reps[i], err = d.Int32()
				if err != nil {
					return err
				}
			}
			ra[pid] = reps
		}
		req.ReplicaAssignment = ra

		configCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		c := make(map[string]string, configCount)
		for j := 0; j < configCount; j++ {
			k, err := d.String()
			if err != nil {
				return err
			}
			v, err := d.String()
			if err != nil {
				return err
			}
			c[k] = v
		}
		req.Configs = c
	}
	c.Timeout, err = d.Int32()

	return err
}

func (c *CreateTopicRequests) Key() int16 {
	return CreateTopicsKey
}

func (c *CreateTopicRequests) Version() int16 {
	return 0
}
