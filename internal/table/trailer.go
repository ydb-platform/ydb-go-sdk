package table

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type trailer struct {
	s  *session
	md metadata.MD
}

func (t *trailer) Trailer() grpc.CallOption {
	return grpc.Trailer(&t.md)
}

func checkHintSessionClose(md metadata.MD) bool {
	for _, hint := range md.Get(meta.HeaderServerHints) {
		if hint == meta.HintSessionClose {
			return true
		}
	}
	return false
}

func (t *trailer) processHints() {
	switch {
	case checkHintSessionClose(t.md):
		t.s.SetStatus(table.SessionClosing)
	default:
		// pass
	}
}
