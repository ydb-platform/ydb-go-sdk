package value

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"
)

func TestDefaultValueRoundTrip(t *testing.T) {
	for _, column := range []*Ydb_Table.ColumnMeta{
		nil,
		{},
		{DefaultValue: &Ydb_Table.ColumnMeta_FromLiteral{FromLiteral: ToYDB(Int64Value(42))}},
		{DefaultValue: &Ydb_Table.ColumnMeta_FromSequence{FromSequence: &Ydb_Table.SequenceDescription{}}},
		{DefaultValue: &Ydb_Table.ColumnMeta_FromSequence{FromSequence: &Ydb_Table.SequenceDescription{
			Name:       proto.String("sequence"),
			MinValue:   proto.Int64(-10),
			MaxValue:   proto.Int64(100),
			StartValue: proto.Int64(5),
			Cache:      proto.Uint64(20),
			Increment:  proto.Int64(2),
			Cycle:      proto.Bool(true),
			SetVal: &Ydb_Table.SequenceDescription_SetVal{
				NextValue: proto.Int64(7),
				NextUsed:  proto.Bool(false),
			},
		}}},
	} {
		t.Run("", func(t *testing.T) {
			got := GetDefaultFromYDB(column)
			if column == nil || column.DefaultValue == nil {
				require.Nil(t, got)

				return
			}
			require.NotNil(t, got)
			if column.GetFromLiteral() != nil {
				require.Equal(t, "42l", got.Literal().Yql())
				require.Nil(t, got.Sequence())
			} else {
				require.NotNil(t, got.Sequence())
				require.Nil(t, got.Literal())
			}
			converted := &Ydb_Table.ColumnMeta{}
			got.ToYDB(converted)
			require.True(t, proto.Equal(column, converted))
			require.NotPanics(t, func() { got.ToYDB(nil) })
		})
	}
}
