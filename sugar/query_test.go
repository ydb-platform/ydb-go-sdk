package sugar_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestUnmarshallRow(t *testing.T) {
	v, err := sugar.UnmarshallRow[rowTestStruct](func() query.Row {
		return newRow(123, "my string")
	}())
	require.NoError(t, err)
	require.EqualValues(t, 123, v.ID)
	require.EqualValues(t, "my string", v.Str)
}

func TestUnmarshallResultSet(t *testing.T) {
	v, err := sugar.UnmarshallResultSet[rowTestStruct](internalQuery.MaterializedResultSet(-1, nil, nil,
		[]query.Row{
			func() query.Row {
				return newRow(123, "my string 1")
			}(),
			func() query.Row {
				return newRow(456, "my string 2")
			}(),
		},
	))
	require.NoError(t, err)
	require.Len(t, v, 2)
	require.EqualValues(t, 123, v[0].ID)
	require.EqualValues(t, "my string 1", v[0].Str)
	require.EqualValues(t, 456, v[1].ID)
	require.EqualValues(t, "my string 2", v[1].Str)
}

type resultStub struct {
	query.Result
	rs  query.ResultSet
	err error
}

func (r resultStub) NextResultSet(context.Context) (query.ResultSet, error) {
	return r.rs, r.err
}

type resultSetStub struct {
	query.ResultSet
	err error
}

func (rs resultSetStub) NextRow(context.Context) (query.Row, error) {
	return nil, rs.err
}

func TestResultErr(t *testing.T) {
	errBroken := errors.New("stream broken")
	for _, tc := range []struct {
		name          string
		r             query.Result
		nextResultSet bool
		err           error
	}{
		{
			name: "no more result sets",
			r:    resultStub{err: io.EOF},
		},
		{
			name: "NextResultSet failed",
			r:    resultStub{err: errBroken},
			err:  errBroken,
		},
		{
			name:          "no more rows",
			r:             resultStub{rs: resultSetStub{err: io.EOF}},
			nextResultSet: true,
		},
		{
			name:          "NextRow failed",
			r:             resultStub{rs: resultSetStub{err: errBroken}},
			nextResultSet: true,
			err:           errBroken,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := sugar.Result(tc.r)
			require.Equal(t, tc.nextResultSet, res.NextResultSet(context.Background()))
			require.False(t, res.NextRow())
			require.ErrorIs(t, res.Err(), tc.err)
		})
	}
}

type rowTestStruct struct {
	ID  uint64 `sql:"id"`
	Str string `sql:"myStr"`
}

// newRow return row for unmarshal to rowTestStruct
func newRow(id uint64, str string) *internalQuery.Row {
	return internalQuery.NewRow([]*Ydb.Column{
		{
			Name: "id",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UINT64,
				},
			},
		},
		{
			Name: "myStr",
			Type: &Ydb.Type{
				Type: &Ydb.Type_TypeId{
					TypeId: Ydb.Type_UTF8,
				},
			},
		},
	}, &Ydb.Value{
		Items: []*Ydb.Value{{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: id,
			},
		}, {
			Value: &Ydb.Value_TextValue{
				TextValue: str,
			},
		}},
	})
}
