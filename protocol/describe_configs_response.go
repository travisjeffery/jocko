package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type DescribeConfigsResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Resources    []DescribeConfigsResourceResponse
}

type DescribeConfigsResourceResponse struct {
	ErrorCode     int16
	ErrorMessage  *string
	Type          int8
	Name          string
	ConfigEntries []DescribeConfigsEntry
}

type DescribeConfigsEntry struct {
	Name        string
	Value       *string
	ReadOnly    bool
	IsDefault   bool
	IsSensitive bool
	Synonyms    []DescribeConfigsSynonym
}

type DescribeConfigsSynonym struct {
	Name   string
	Value  *string
	Source int8
}

func (r *DescribeConfigsResponse) Encode(e PacketEncoder) error {
	e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	if err := e.PutArrayLength(len(r.Resources)); err != nil {
		return err
	}
	for _, resource := range r.Resources {
		e.PutInt16(resource.ErrorCode)
		if err := e.PutNullableString(resource.ErrorMessage); err != nil {
			return err
		}
		e.PutInt8(resource.Type)
		if err := e.PutString(resource.Name); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(resource.ConfigEntries)); err != nil {
			return err
		}
		for _, entry := range resource.ConfigEntries {
			if err := e.PutString(entry.Name); err != nil {
				return err
			}
			if err := e.PutNullableString(entry.Value); err != nil {
				return err
			}
			e.PutBool(entry.ReadOnly)
			e.PutBool(entry.IsDefault)
			e.PutBool(entry.IsSensitive)
			if r.APIVersion >= 1 {
				if err := e.PutArrayLength(len(entry.Synonyms)); err != nil {
					return err
				}
				for _, synonym := range entry.Synonyms {
					if err := e.PutString(synonym.Name); err != nil {
						return err
					}
					if err := e.PutNullableString(synonym.Value); err != nil {
						return err
					}
					e.PutInt8(synonym.Source)
				}
			}
		}
	}
	return nil
}

func (r *DescribeConfigsResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	throttle, err := d.Int32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	resourceCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Resources = make([]DescribeConfigsResourceResponse, resourceCount)
	for i := 0; i < resourceCount; i++ {
		resource := DescribeConfigsResourceResponse{}
		if resource.ErrorCode, err = d.Int16(); err != nil {
			return err
		}
		if resource.ErrorMessage, err = d.NullableString(); err != nil {
			return err
		}
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
		entries := make([]DescribeConfigsEntry, entryCount)
		for j := 0; j < entryCount; j++ {
			entry := DescribeConfigsEntry{}
			if entry.Name, err = d.String(); err != nil {
				return err
			}
			if entry.Value, err = d.NullableString(); err != nil {
				return err
			}
			if entry.ReadOnly, err = d.Bool(); err != nil {
				return err
			}
			if entry.IsDefault, err = d.Bool(); err != nil {
				return err
			}
			if entry.IsSensitive, err = d.Bool(); err != nil {
				return err
			}
			if version >= 1 {
				synonymsCount, err := d.ArrayLength()
				if err != nil {
					return err
				}
				synonyms := make([]DescribeConfigsSynonym, synonymsCount)
				for k := 0; k < synonymsCount; k++ {
					synonym := DescribeConfigsSynonym{}
					if synonym.Name, err = d.String(); err != nil {
						return err
					}
					if synonym.Value, err = d.NullableString(); err != nil {
						return err
					}
					if synonym.Source, err = d.Int8(); err != nil {
						return err
					}
					synonyms[k] = synonym
				}
				entry.Synonyms = synonyms
			}
			entries[j] = entry

		}
		resource.ConfigEntries = entries
		r.Resources[i] = resource
	}
	return nil
}

func (r *DescribeConfigsResponse) Version() int16 {
	return r.APIVersion
}

func (r *DescribeConfigsResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
