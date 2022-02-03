package table

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type trailer struct {
	s  *session
	md metadata.MD
}

func (t *trailer) Trailer() grpc.CallOption {
	return grpc.Trailer(&t.md)
}

func checkHintSessionClose(md metadata.MD) bool {
	for _, hint := range md.Get(meta.MetaServerHints) {
		if hint == meta.MetaSessionClose {
			return true
		}
	}
	return false
}

func (t *trailer) Check() {
	if checkHintSessionClose(t.md) {
		t.s.SetStatus(options.SessionClosing)
	}
}
