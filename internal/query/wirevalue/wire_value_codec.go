package wirevalue

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

type codec struct {
	protobuf encoding.CodecV2
}

// NewCodec returns a per-call protobuf codec that decodes responses into Part.
func NewCodec() encoding.CodecV2 {
	return codec{protobuf: encoding.GetCodecV2("proto")}
}

func (c codec) Name() string { return "proto" }

func (c codec) Marshal(v any) (mem.BufferSlice, error) {
	return c.protobuf.Marshal(v)
}

func (c codec) Unmarshal(data mem.BufferSlice, v any) error {
	if part, ok := v.(*Part); ok {
		frame := make([]byte, data.Len())
		data.CopyTo(frame)
		decoded, err := decodeOwnedPart(frame)
		if err != nil {
			return err
		}
		*part = *decoded
		return nil
	}
	return c.protobuf.Unmarshal(data, v)
}
