package scanner

import (
	"fmt"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type (
	NamedScanner struct {
		data *data
	}
	NamedDestination struct {
		name string
		ref  interface{}
	}
)

func NamedRef(columnName string, destinationValueReference interface{}) (dst NamedDestination) {
	if columnName == "" {
		panic("columnName must be not empty")
	}
	dst.name = columnName
	v := reflect.TypeOf(destinationValueReference)
	if v.Kind() != reflect.Ptr {
		panic(fmt.Errorf("%T is not reference type", destinationValueReference))
	}
	dst.ref = destinationValueReference

	return dst
}

func Named(data *data) NamedScanner {
	return NamedScanner{
		data: data,
	}
}

func (s NamedScanner) ScanNamed(dst ...NamedDestination) error {
	for i := range dst {
		v, err := s.data.seekByName(dst[i].name)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		if err = value.CastTo(v, dst[i].ref); err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	return nil
}
