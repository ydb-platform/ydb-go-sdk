package scanner

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type IndexedScanner struct {
	data *Data
}

func Indexed(data *Data) IndexedScanner {
	return IndexedScanner{
		data: data,
	}
}

func (s IndexedScanner) Scan(dst ...any) (err error) {
	if len(dst) != len(s.data.columns) {
		return xerrors.WithStackTrace(
			fmt.Errorf("%w: %d != %d",
				errIncompatibleColumnsAndDestinations,
				len(dst), len(s.data.columns),
			),
		)
	}
	for i := range dst {
		if err := s.data.scanByIndex(i, dst[i]); err != nil {
			return xerrors.WithStackTrace(fmt.Errorf("scan error on column index %d: %w", i, err))
		}
	}

	return nil
}
