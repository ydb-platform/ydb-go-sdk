package query

import (
	"fmt"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.NamedScanner = scannerNamed{}

type scannerNamed struct {
	data *scannerData
}

func newScannerNamed(data *scannerData) scannerNamed {
	return scannerNamed{
		data: data,
	}
}

func (s scannerNamed) seekByName(name string) (
	value.Value,
	error,
) {
	for i := range s.data.columns {
		if s.data.columns[i].GetName() == name {
			return value.FromYDB(s.data.columns[i].GetType(), s.data.values[i]), nil
		}
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf("'%s': %w", name, errColumnNotFoundByName))
}

func (s scannerNamed) ScanNamed(dst ...query.NamedDestination) error {
	if len(dst) != len(s.data.columns) {
		return xerrors.WithStackTrace(errWrongArgumentsCount)
	}

	for i := range dst {
		v, err := s.seekByName(dst[i].Name())
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		if err := value.CastTo(v, dst[i].Destination()); err != nil {
			to := reflect.ValueOf(dst[i].Destination())
			if to.Kind() != reflect.Pointer {
				return xerrors.WithStackTrace(
					fmt.Errorf("dst[%d].Destination() type is not a pointer ('%s')", i,
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
