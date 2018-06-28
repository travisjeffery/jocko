package protocol

import "time"

type CreateTopicRequest struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	Configs           map[string]*string
}

type CreateTopicRequests struct {
	APIVersion int16

	Requests     []*CreateTopicRequest
	Timeout      time.Duration
	ValidateOnly bool
}

func (r *CreateTopicRequests) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Requests)); err != nil {
		return err
	}
	for _, r := range r.Requests {
		if err = e.PutString(r.Topic); err != nil {
			return err
		}
		e.PutInt32(r.NumPartitions)
		e.PutInt16(r.ReplicationFactor)
		if err = e.PutArrayLength(len(r.ReplicaAssignment)); err != nil {
			return err
		}
		for pid, ass := range r.ReplicaAssignment {
			e.PutInt32(pid)
			if err = e.PutInt32Array(ass); err != nil {
				return err
			}
		}
		if err = e.PutArrayLength(len(r.Configs)); err != nil {
			return err
		}
		for k, v := range r.Configs {
			if err = e.PutString(k); err != nil {
				return err
			}
			if err = e.PutNullableString(v); err != nil {
				return err
			}
		}
	}
	e.PutInt32(int32(r.Timeout / time.Millisecond))
	if r.APIVersion >= 1 {
		e.PutBool(r.ValidateOnly)
	}
	return nil
}

func (r *CreateTopicRequests) Decode(d PacketDecoder, version int16) error {
	var err error
	requestCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Requests = make([]*CreateTopicRequest, requestCount)
	for i := range r.Requests {
		req := new(CreateTopicRequest)
		r.Requests[i] = req
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
		if err != nil {
			return err
		}
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
		r := make(map[string]*string, configCount)
		for j := 0; j < configCount; j++ {
			k, err := d.String()
			if err != nil {
				return err
			}
			v, err := d.NullableString()
			if err != nil {
				return err
			}
			r[k] = v
		}
		req.Configs = r
	}
	timeout, err := d.Int32()
	if err != nil {
		return err
	}
	r.Timeout = time.Duration(timeout) * time.Millisecond
	if version >= 1 {
		r.ValidateOnly, err = d.Bool()
		if err != nil {
			return nil
		}
	}
	return nil
}

func (r *CreateTopicRequests) Key() int16 {
	return CreateTopicsKey
}

func (r *CreateTopicRequests) Version() int16 {
	return r.APIVersion
}
