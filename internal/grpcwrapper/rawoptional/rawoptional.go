package rawoptional

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Bool struct {
	Value    bool
	HasValue bool
}

func (b *Bool) ToProto() *bool {
	if !b.HasValue {
		return nil
	}

	val := b.Value

	return &val
}

type Duration struct {
	Value    time.Duration
	HasValue bool
}

func (v *Duration) ToProto() *durationpb.Duration {
	if v.HasValue {
		return durationpb.New(v.Value)
	}

	return nil
}

type Int64 struct {
	Value    int64
	HasValue bool
}

func (v *Int64) ToProto() *int64 {
	if !v.HasValue {
		return nil
	}

	val := v.Value

	return &val
}

type Time struct {
	Value    time.Time
	HasValue bool
}

func (v *Time) ToProto() *timestamppb.Timestamp {
	if v.HasValue {
		return timestamppb.New(v.Value)
	}

	return nil
}

func (v *Time) MustFromProto(proto *timestamppb.Timestamp) {
	if proto == nil {
		v.Value = time.Time{}
		v.HasValue = false

		return
	}

	v.HasValue = true
	v.Value = proto.AsTime()
}
