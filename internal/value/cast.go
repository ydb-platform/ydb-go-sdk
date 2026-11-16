package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func CastTo(v Value, dst any) error {
	if dst == nil {
		return errNilDestination
	}

	switch x := dst.(type) {
	case *Value:
		*x = v

		return nil
	case **Value:
		*x = &v

		return nil
	case *Ydb.Value:
		proto.Reset(x)
		proto.Merge(x, v.toYDB())

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
