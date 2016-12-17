package protocol

type DescribeGroupsRequest struct {
	GroupIDs []string
}

func (r *DescribeGroupsRequest) Encode(e PacketEncoder) error {
	return e.PutStringArray(r.GroupIDs)
}

func (r *DescribeGroupsRequest) Decode(d PacketDecoder) (err error) {
	r.GroupIDs, err = d.StringArray()
	return err
}

func (r *DescribeGroupsRequest) Key() int16 {
	return DescribeGroupsKey
}

func (r *DescribeGroupsRequest) Version() int16 {
	return 0
}
