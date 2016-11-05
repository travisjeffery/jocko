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

func (c *CreateTopicRequests) Decode(d Decoder) error {
	var err error
	reqslen, err := d.ArrayLength()
	if err != nil {
		return err
	}
	c.Requests = make([]*CreateTopicRequest, reqslen)
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
		ralen, err := d.ArrayLength()
		ra := make(map[int32][]int32, ralen)
		for i := 0; i < ralen; i++ {
			pid, err := d.Int32()
			if err != nil {
				return err
			}
			replen, err := d.ArrayLength()
			if err != nil {
				return err
			}
			reps := make([]int32, replen)
			for i := range reps {
				reps[i], err = d.Int32()
				if err != nil {
					return err
				}
			}
			ra[pid] = reps
		}
		req.ReplicaAssignment = ra

		clen, err := d.ArrayLength()
		if err != nil {
			return err
		}
		c := make(map[string]string, clen)
		for j := 0; j < clen; j++ {
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

	return nil
}

func (c *CreateTopicRequests) encode(e Encoder) {
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
}

func (c *CreateTopicRequests) key() int16 {
	return 19
}

func (c *CreateTopicRequests) version() int16 {
	return 0
}

func (c *CreateTopicRequests) Encode() ([]byte, error) {
	rh := &RequestHeader{
		APIKey:        19,
		APIVersion:    0,
		CorrelationID: CorrelationID(),
		ClientID:      ClientID(),
	}
	le := &LenEncoder{}
	rh.Encode(le)
	rhlen := le.Length
	b := make([]byte, rhlen)
	e := NewByteEncoder(b)
	rh.Encode(e)
	rhb := e.Bytes()

	le = &LenEncoder{}
	c.encode(le)
	l := le.Length + rhlen
	b = make([]byte, l)

	e = NewByteEncoder(b)
	e.PutRawBytes(rhb)
	c.encode(e)

	Encoding.PutUint32(b, uint32(l))

	return b, nil
}
