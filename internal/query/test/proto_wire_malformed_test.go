package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wirevalue"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryRejectsMalformedUnusedValueMessage(t *testing.T) {
	nested := new(Ydb.Value)
	nested.ProtoReflect().SetUnknown(protowire.AppendBytes(
		protowire.AppendTag(nil, wirevalue.ValueNestedField, wirevalue.ValueNestedWireType), []byte{0xff},
	))
	badItems := protowire.AppendBytes(
		protowire.AppendTag(nil, wirevalue.ValueItemsField, wirevalue.ValueItemsWireType), []byte{0xff},
	)
	number := value.ToYDB(value.Uint64Value(42)).GetValue()
	number.ProtoReflect().SetUnknown(badItems)
	null := value.ToYDB(value.NullValue(types.Int32)).GetValue()
	null.ProtoReflect().SetUnknown(badItems)
	for _, tt := range []struct {
		name        string
		typeOf      types.Type
		cell        *Ydb.Value
		destination func() any
	}{
		{name: "nested", typeOf: types.Int32, cell: nested, destination: func() any { return new(int32) }},
		{name: "native uint64", typeOf: types.Uint64, cell: number, destination: func() any { return new(uint64) }},
		{
			name: "native optional null", typeOf: types.NewOptional(types.Int32), cell: null,
			destination: func() any { return new(*int32) },
		},
	} {
		for _, wire := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wire=%v", tt.name, wire), func(t *testing.T) {
				part := &Ydb_Query.ExecuteQueryResponsePart{
					Status: Ydb.StatusIds_SUCCESS,
					ResultSet: &Ydb.ResultSet{
						Columns: []*Ydb.Column{{Name: "value", Type: tt.typeOf.ToYDB()}},
						Rows:    []*Ydb.Value{{Items: []*Ydb.Value{tt.cell}}},
					},
				}
				server := mock.Server(t, mock.WithExecuteQueryParts(part))
				db, err := ydb.Open(t.Context(), server.ConnString(), ydb.WithAnonymousCredentials(),
					ydb.WithQueryProtoWireDecoding(wire))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })
				got := tt.destination()
				var accepted bool
				err = db.Query().Do(t.Context(), func(ctx context.Context, session query.Session) error {
					result, err := session.Query(ctx, "SELECT malformed fixture")
					if err != nil {
						return err
					}
					defer result.Close(ctx)
					set, err := result.NextResultSet(ctx)
					if err != nil {
						return err
					}
					row, err := set.NextRow(ctx)
					if err != nil {
						return err
					}
					err = row.Scan(got)
					accepted = err == nil

					return err
				})
				if accepted {
					t.Fatalf("Scan accepted a malformed column into %T", got)
				}
				require.ErrorContains(t, err, "cannot parse invalid wire-format data")
				require.Equal(t, uint64(1), server.ExecuteQueryCalls())
			})
		}
	}
}
