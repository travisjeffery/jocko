package protocol

type MetadataRequest struct {
	APIVersion int16

	Topics                 []string
	AllowAutoTopicCreation bool
}

func (r *MetadataRequest) Encode(e PacketEncoder) (err error) {
	err = e.PutStringArray(r.Topics)
	if err != nil {
		return err
	}
	if r.APIVersion >= 4 {
		e.PutBool(r.AllowAutoTopicCreation)
	}
	return nil
}

func (r *MetadataRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	r.Topics, err = d.StringArray()
	if err != nil {
		return err
	}
	if version >= 4 {
		r.AllowAutoTopicCreation, err = d.Bool()
	}
	return err
}

func (r *MetadataRequest) Key() int16 {
	return MetadataKey
}

func (r *MetadataRequest) Version() int16 {
	return r.APIVersion
}

type Strings []string
