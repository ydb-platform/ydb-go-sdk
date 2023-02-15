package types

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errNilValue = errors.New("nil value")

// CastTo try cast value to destination type value
func CastTo(v Value, dst interface{}) error {
	if v == nil {
		return xerrors.WithStackTrace(errNilValue)
	}
	return value.CastTo(v, dst)
}

func TupleItems(v Value) ([]Value, error) {
	if vv, has := v.(interface {
		Items() []Value
	}); has {
		return vv.Items(), nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("cannot get tuple items from '%s'", v.Type().Yql()))
}

func StructFields(v Value) (map[string]Value, error) {
	if vv, has := v.(interface {
		Fields() map[string]Value
	}); has {
		return vv.Fields(), nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("cannot get struct fields from '%s'", v.Type().Yql()))
}

func DictFields(v Value) (map[Value]Value, error) {
	if vv, has := v.(interface {
		Values() map[Value]Value
	}); has {
		return vv.Values(), nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("cannot get dict values from '%s'", v.Type().Yql()))
}
