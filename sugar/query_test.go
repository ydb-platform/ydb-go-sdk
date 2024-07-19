package sugar_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ query.Client = (*mockReadRowClient)(nil)
	_ query.Client = (*mockReadResultSetClient)(nil)
)

type mockReadResultSetClient struct {
	rs query.ResultSet
}

func (c *mockReadResultSetClient) Stats() *pool.Stats {
	return nil
}

func (c *mockReadResultSetClient) Execute(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.Result, error) {
	panic("unexpected call")
}

func (c *mockReadResultSetClient) Do(
	ctx context.Context, op query.Operation, opts ...options.DoOption,
) error {
	panic("unexpected call")
}

func (c *mockReadResultSetClient) DoTx(
	ctx context.Context, op query.TxOperation, opts ...options.DoTxOption,
) error {
	panic("unexpected call")
}

func (c *mockReadResultSetClient) ReadRow(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.Row, error) {
	panic("unexpected call")
}

func (c *mockReadResultSetClient) ReadResultSet(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.ResultSet, error) {
	return c.rs, nil
}

type mockReadRowClient struct {
	row query.Row
}

func (c *mockReadRowClient) Stats() *pool.Stats {
	return nil
}

func (c *mockReadRowClient) Execute(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.Result, error) {
	panic("unexpected call")
}

func (c *mockReadRowClient) Do(
	ctx context.Context, op query.Operation, opts ...options.DoOption,
) error {
	panic("unexpected call")
}

func (c *mockReadRowClient) DoTx(
	ctx context.Context, op query.TxOperation, opts ...options.DoTxOption,
) error {
	panic("unexpected call")
}

func (c *mockReadRowClient) ReadRow(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.Row, error) {
	return c.row, nil
}

func (c *mockReadRowClient) ReadResultSet(
	ctx context.Context, query string, opts ...options.ExecuteOption,
) (query.ResultSet, error) {
	panic("unexpected call")
}

func TestUnmarshallRow(t *testing.T) {
	ctx := xtest.Context(t)
	type myStruct struct {
		ID  uint64 `sql:"id"`
		Str string `sql:"myStr"`
	}
	v, err := sugar.UnmarshallRow[myStruct](func() query.Row {
		row, err := internalQuery.NewRow(ctx, []*Ydb.Column{
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
					Uint64Value: 123,
				},
			}, {
				Value: &Ydb.Value_TextValue{
					TextValue: "my string",
				},
			}},
		}, &trace.Query{})
		if err != nil {
			panic(err)
		}

		return row
	}())
	require.NoError(t, err)
	require.EqualValues(t, 123, v.ID)
	require.EqualValues(t, "my string", v.Str)
}

func TestUnmarshallResultSet(t *testing.T) {
	ctx := xtest.Context(t)
	type myStruct struct {
		ID  uint64 `sql:"id"`
		Str string `sql:"myStr"`
	}
	v, err := sugar.UnmarshallResultSet[myStruct](internalQuery.NewMaterializedResultSet(-1, nil, nil,
		[]query.Row{
			func() query.Row {
				row, err := internalQuery.NewRow(ctx, []*Ydb.Column{
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
							Uint64Value: 123,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "my string 1",
						},
					}},
				}, &trace.Query{})
				if err != nil {
					panic(err)
				}

				return row
			}(),
			func() query.Row {
				row, err := internalQuery.NewRow(ctx, []*Ydb.Column{
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
							Uint64Value: 456,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "my string 2",
						},
					}},
				}, &trace.Query{})
				if err != nil {
					panic(err)
				}

				return row
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
