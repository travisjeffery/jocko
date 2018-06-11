package protocol

import (
	"go.uber.org/zap/zapcore"
)

// https://kafka.apache.org/protocol#The_Messages_DescribeConfigs

type DescribeConfigsRequest struct {
	APIVersion int16

	Resources       []DescribeConfigsResource
	IncludeSynonyms bool
}

type DescribeConfigsResource struct {
	Type        int8
	Name        string
	ConfigNames []string
}

func (r *DescribeConfigsRequest) Encode(e PacketEncoder) (err error) {
	if err := e.PutArrayLength(len(r.Resources)); err != nil {
		return err
	}
	for _, resource := range r.Resources {
		e.PutInt8(resource.Type)
		if err = e.PutString(resource.Name); err != nil {
			return err
		}
		if err = e.PutStringArray(resource.ConfigNames); err != nil {
			return err
		}
	}
	if r.APIVersion >= 1 {
		e.PutBool(r.IncludeSynonyms)
	}
	return nil
}

func (r *DescribeConfigsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	n, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Resources = make([]DescribeConfigsResource, n)
	for i := 0; i < n; i++ {
		resource := DescribeConfigsResource{}
		if resource.Type, err = d.Int8(); err != nil {
			return err
		}
		if resource.Name, err = d.String(); err != nil {
			return err
		}
		if resource.ConfigNames, err = d.StringArray(); err != nil {
			return err
		}
		r.Resources[i] = resource
	}
	if version >= 1 {
		if r.IncludeSynonyms, err = d.Bool(); err != nil {
			return err
		}
	}
	return nil
}

func (r *DescribeConfigsRequest) Key() int16 {
	return DescribeConfigsKey
}

func (r *DescribeConfigsRequest) Version() int16 {
	return r.APIVersion
}

func (r *DescribeConfigsRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
