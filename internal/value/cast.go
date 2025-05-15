package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func CastTo(v Value, dst interface{}) error {
	if dst == nil {
		return errNilDestination
	}
	
	switch x := dst.(type) {
	case *Value:
		*x = v

		return nil
	case **Value:
		**x = v

		return nil
	case *Ydb.Value:
		*x = *v.toYDB()

		return nil
	case **Ydb.Value:
		*x = v.toYDB()

		return nil
	case *Ydb.TypedValue:
		x.Type = v.Type().ToYDB()
		x.Value = v.toYDB()

		return nil
	case **Ydb.TypedValue:
		*x = &Ydb.TypedValue{
			Type:  v.Type().ToYDB(),
			Value: v.toYDB(),
		}

		return nil
	default:
		return v.castTo(dst)
	}
}
