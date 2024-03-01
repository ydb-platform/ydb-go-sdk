package query

import (
	"fmt"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.IndexedScanner = scannerIndexed{}

type scannerIndexed struct {
	data *scannerData
}

func newScannerIndexed(data *scannerData) scannerIndexed {
	return scannerIndexed{
		data: data,
	}
}

func (s scannerIndexed) Scan(dst ...interface{}) error {
	if len(dst) != len(s.data.columns) {
		return xerrors.WithStackTrace(errWrongArgumentsCount)
	}

	for i := range dst {
		v := value.FromYDB(s.data.columns[i].GetType(), s.data.values[i])
		if err := value.CastTo(v, dst[i]); err != nil {
			to := reflect.ValueOf(dst[i])
			if to.Kind() != reflect.Pointer {
				return xerrors.WithStackTrace(
					fmt.Errorf("dst[%d] type is not a pointer ('%s')", i,
						to.Kind().String(),
					))
			}
			vv := reflect.ValueOf(v)
			if vv.CanConvert(to.Elem().Type()) {
				to.Elem().Set(vv.Convert(to.Elem().Type()))
			} else {
				return xerrors.WithStackTrace(err)
			}
		}
	}

	return nil
}
