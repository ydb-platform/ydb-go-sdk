package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

type codec struct {
	protobuf encoding.CodecV2
	capture  **wirePart
}

// newWireCodec returns a per-call protobuf codec that can capture wire rows.
func newWireCodec() *codec {
	return &codec{protobuf: encoding.GetCodecV2("proto")}
}

func (c *codec) Name() string { return "proto" }

func (c *codec) Marshal(v any) (mem.BufferSlice, error) {
	return c.protobuf.Marshal(v)
}

func (c *codec) Unmarshal(data mem.BufferSlice, v any) error {
	if part, ok := v.(*Ydb_Query.ExecuteQueryResponsePart); ok && c.capture != nil {
		frame := make([]byte, data.Len())
		data.CopyTo(frame)
		decoded, err := decodeOwnedPart(frame, part)
		if err != nil {
			return err
		}
		*c.capture = decoded

		return nil
	}

	return c.protobuf.Unmarshal(data, v)
}
