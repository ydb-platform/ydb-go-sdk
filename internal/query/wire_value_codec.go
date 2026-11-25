package query

import (
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

type codec struct {
	protobuf encoding.CodecV2
	capture  atomic.Pointer[wireReceive]
}

type wireReceive struct {
	part Ydb_Query.ExecuteQueryResponsePart
	wire *wirePart
}

func (c *codec) Name() string { return "proto" }

func (c *codec) Marshal(v any) (mem.BufferSlice, error) {
	return c.protobuf.Marshal(v)
}

func (c *codec) Unmarshal(data mem.BufferSlice, v any) error {
	if capture := c.capture.Load(); capture != nil && v == &capture.part {
		frame := make([]byte, data.Len())
		data.CopyTo(frame)
		decoded, err := decodeOwnedPart(frame, &capture.part)
		if err != nil {
			return err
		}
		capture.wire = decoded

		return nil
	}

	return c.protobuf.Unmarshal(data, v)
}
