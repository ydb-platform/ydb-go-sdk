package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func Stats(client Client) (*stats.Stats, error) {
	if c, has := client.(interface {
		Stats() *stats.Stats
	}); has {
		return c.Stats(), nil
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf("client %T not supported stats", client)) //nolint:goerr113
}
