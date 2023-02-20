package bind

import (
	"database/sql"
	"sort"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type Declare struct {
	Name string
	Type string
}

func (d Declare) String() string {
	return "DECLARE " + d.Name + " AS " + d.Type
}

func (b Bindings) declares(params *table.QueryParameters) (declares []Declare) {
	if !b.AllowBindParams {
		return nil
	}
	params.Each(func(name string, v types.Value) {
		declares = append(declares, Declare{
			Name: name,
			Type: v.Type().Yql(),
		})
	})
	sort.Slice(declares, func(i, j int) bool {
		return declares[i].Name < declares[j].Name
	})
	return declares
}

func GenerateDeclareSection(args []sql.NamedArg) (string, error) {
	values := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		if arg.Name == "" {
			return "", xerrors.WithStackTrace(internal.ErrNameRequired)
		}
		value, err := ToValue(arg.Value)
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}
		values[i] = table.ValueParam(arg.Name, value)
	}
	return internal.GenerateDeclareSection(table.NewQueryParameters(values...))
}
