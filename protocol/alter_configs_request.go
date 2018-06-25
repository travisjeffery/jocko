package protocol

type AlterConfigsRequest struct {
	APIVersion int16

	Resources    []AlterConfigsResource
	ValidateOnly bool
}

type AlterConfigsResource struct {
	Type    int8
	Name    string
	Entries []AlterConfigsEntry
}

type AlterConfigsEntry struct {
	Name  string
	Value *string
}

func (r *AlterConfigsRequest) Encode(e PacketEncoder) (err error) {
	if err := e.PutArrayLength(len(r.Resources)); err != nil {
		return err
	}
	for _, resource := range r.Resources {
		e.PutInt8(resource.Type)
		if err := e.PutString(resource.Name); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(resource.Entries)); err != nil {
			return err
		}
		for _, entry := range resource.Entries {
			if err := e.PutString(entry.Name); err != nil {
				return err
			}
			if err := e.PutNullableString(entry.Value); err != nil {
				return err
			}
		}
	}
	e.PutBool(r.ValidateOnly)
	return nil
}

func (r *AlterConfigsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	resourceCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Resources = make([]AlterConfigsResource, resourceCount)
	for i := 0; i < resourceCount; i++ {
		resource := AlterConfigsResource{}
		if resource.Type, err = d.Int8(); err != nil {
			return err
		}
		if resource.Name, err = d.String(); err != nil {
			return err
		}
		entryCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		resource.Entries = make([]AlterConfigsEntry, entryCount)
		for j := 0; j < entryCount; j++ {
			entry := AlterConfigsEntry{}
			if entry.Name, err = d.String(); err != nil {
				return err
			}
			if entry.Value, err = d.NullableString(); err != nil {
				return err
			}
			resource.Entries[j] = entry
		}
		r.Resources[i] = resource
	}
	if r.ValidateOnly, err = d.Bool(); err != nil {
		return err
	}
	return nil
}

func (r *AlterConfigsRequest) Key() int16 {
	return AlterConfigsKey
}

func (r *AlterConfigsRequest) Version() int16 {
	return r.APIVersion
}
