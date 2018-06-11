package protocol

import (
	"go.uber.org/zap/zapcore"
)

type DescribeGroupsRequest struct {
	APIVersion int16

	GroupIDs []string
}

func (r *DescribeGroupsRequest) Encode(e PacketEncoder) error {
	return e.PutStringArray(r.GroupIDs)
}

func (r *DescribeGroupsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	r.GroupIDs, err = d.StringArray()
	return err
}

func (r *DescribeGroupsRequest) Key() int16 {
	return DescribeGroupsKey
}

func (r *DescribeGroupsRequest) Version() int16 {
	return r.APIVersion
}

func (r *DescribeGroupsRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
