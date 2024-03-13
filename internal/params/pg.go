package params

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

type pg struct {
	param *Parameter
}

func (p *pg) Unknown(val string) Builder {
	p.param.value = value.PgUnknownValue(val)
	p.param.parent.params = append(p.param.parent.params, p.param)

	return p.param.parent
}
