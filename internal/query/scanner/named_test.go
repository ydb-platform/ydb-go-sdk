package scanner

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestNamed(t *testing.T) {
	for _, tt := range []struct {
		name string
		s    NamedScanner
		dst  [][]interface{}
		exp  [][]interface{}
	}{
		{
			name: "Ydb.Type_UTF8",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UTF8,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_TextValue{
							TextValue: "test",
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v string) *string { return &v }("")},
				{func(v []byte) *[]byte { return &v }([]byte(""))},
			},
			exp: [][]interface{}{
				{func(v string) *string { return &v }("test")},
				{func(v []byte) *[]byte { return &v }([]byte("test"))},
			},
		},
		{
			name: "Ydb.Type_STRING",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_STRING,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_BytesValue{
							BytesValue: []byte("test"),
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v string) *string { return &v }("")},
				{func(v []byte) *[]byte { return &v }([]byte(""))},
			},
			exp: [][]interface{}{
				{func(v string) *string { return &v }("test")},
				{func(v []byte) *[]byte { return &v }([]byte("test"))},
			},
		},
		{
			name: "Ydb.Type_UINT64",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_INT64",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_INT64,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v int64) *int64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v int64) *int64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_UINT32",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT32,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v int64) *int64 { return &v }(0)},
				{func(v uint32) *uint32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(123)},
				{func(v int64) *int64 { return &v }(123)},
				{func(v uint32) *uint32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_INT32",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_INT32,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v int64) *int64 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v int) *int { return &v }(0)},
				{func(v float32) *float32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v int64) *int64 { return &v }(123)},
				{func(v int32) *int32 { return &v }(123)},
				{func(v int) *int { return &v }(123)},
				{func(v float32) *float32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_UINT16",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT16,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v int64) *int64 { return &v }(0)},
				{func(v uint32) *uint32 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v float32) *float32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(123)},
				{func(v int64) *int64 { return &v }(123)},
				{func(v uint32) *uint32 { return &v }(123)},
				{func(v int32) *int32 { return &v }(123)},
				{func(v float32) *float32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_INT16",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_INT16,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v int64) *int64 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v float32) *float32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v int64) *int64 { return &v }(123)},
				{func(v int32) *int32 { return &v }(123)},
				{func(v float32) *float32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_UINT8",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT8,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v int64) *int64 { return &v }(0)},
				{func(v uint32) *uint32 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v uint8) *uint8 { return &v }(0)},
				{func(v float32) *float32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(123)},
				{func(v int64) *int64 { return &v }(123)},
				{func(v uint32) *uint32 { return &v }(123)},
				{func(v int32) *int32 { return &v }(123)},
				{func(v uint8) *uint8 { return &v }(123)},
				{func(v float32) *float32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_INT8",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_INT8,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v int64) *int64 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v int8) *int8 { return &v }(0)},
				{func(v float32) *float32 { return &v }(0)},
				{func(v float64) *float64 { return &v }(0)},
			},
			exp: [][]interface{}{
				{func(v int64) *int64 { return &v }(123)},
				{func(v int32) *int32 { return &v }(123)},
				{func(v int8) *int8 { return &v }(123)},
				{func(v float32) *float32 { return &v }(123)},
				{func(v float64) *float64 { return &v }(123)},
			},
		},
		{
			name: "Ydb.Type_BOOL",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_BOOL,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v bool) *bool { return &v }(false)},
			},
			exp: [][]interface{}{
				{func(v bool) *bool { return &v }(true)},
			},
		},
		{
			name: "Ydb.Type_DATE",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_DATE,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 100500,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v int64) *int64 { return &v }(0)},
				{func(v int32) *int32 { return &v }(0)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(0, 0))},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(100500)},
				{func(v int64) *int64 { return &v }(100500)},
				{func(v int32) *int32 { return &v }(100500)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(8683200000, 0).UTC())},
			},
		},
		{
			name: "Ydb.Type_DATETIME",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_DATETIME,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 100500,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v int64) *int64 { return &v }(0)},
				{func(v uint32) *uint32 { return &v }(0)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(0, 0))},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(100500)},
				{func(v int64) *int64 { return &v }(100500)},
				{func(v uint32) *uint32 { return &v }(100500)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(100500, 0))},
			},
		},
		{
			name: "Ydb.Type_TIMESTAMP",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_TIMESTAMP,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 12345678987654321,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(0)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(0, 0))},
			},
			exp: [][]interface{}{
				{func(v uint64) *uint64 { return &v }(12345678987654321)},
				{func(v time.Time) *time.Time { return &v }(time.Unix(12345678987, 654321000))},
			},
		},
		{
			name: "Ydb.Type_INTERVAL",
			s: Named(Data(
				[]*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_INTERVAL,
							},
						},
					},
				},
				[]*Ydb.Value{
					{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 100500,
						},
					},
				},
			)),
			dst: [][]interface{}{
				{func(v int64) *int64 { return &v }(0)},
				{func(v time.Duration) *time.Duration { return &v }(time.Duration(0))},
			},
			exp: [][]interface{}{
				{func(v int64) *int64 { return &v }(100500)},
				{func(v time.Duration) *time.Duration { return &v }(time.Duration(100500000))},
			},
		},
	} {
		for i := range tt.dst {
			t.Run(tt.name+"→"+reflect.TypeOf(tt.dst[i][0]).Elem().String(), func(t *testing.T) {
				err := tt.s.ScanNamed(func() (dst []NamedDestination) {
					for j := range tt.dst[i] {
						dst = append(dst, NamedRef("a", tt.dst[i][j]))
					}

					return dst
				}()...)
				require.NoError(t, err)
				require.Equal(t, tt.exp[i], tt.dst[i])
			})
		}
	}
}

func TestScannerNamedNotFoundByName(t *testing.T) {
	scanner := Named(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var s string
	err := scanner.ScanNamed(NamedRef("b", &s))
	require.ErrorIs(t, err, ErrColumnsNotFoundInRow)
}

func TestScannerNamedOrdering(t *testing.T) {
	scanner := Named(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "b",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
			{
				Name: "c",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "A",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "B",
				},
			},
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "C",
				},
			},
		},
	))
	var a, b, c string
	err := scanner.ScanNamed(
		NamedRef("c", &c),
		NamedRef("b", &b),
		NamedRef("a", &a),
	)
	require.NoError(t, err)
	require.Equal(t, "A", a)
	require.Equal(t, "B", b)
	require.Equal(t, "C", c)
}

func TestNamedRef(t *testing.T) {
	for _, tt := range []struct {
		name  string
		ref   interface{}
		dst   NamedDestination
		panic bool
	}{
		{
			name:  "",
			ref:   nil,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name:  "nil_ref",
			ref:   nil,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name:  "not_ref",
			ref:   123,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name: "int_ptr",
			ref:  func(v int) *int { return &v }(123),
			dst: namedDestination{
				name: "int_ptr",
				ref:  func(v int) *int { return &v }(123),
			},
			panic: false,
		},
		{
			name: "int_dbl_ptr",
			ref: func(v int) **int {
				vv := &v

				return &vv
			}(123),
			dst: namedDestination{
				name: "int_dbl_ptr",
				ref: func(v int) **int {
					vv := &v

					return &vv
				}(123),
			},
			panic: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panic {
				defer func() {
					require.NotNil(t, recover())
				}()
			} else {
				defer func() {
					require.Nil(t, recover())
				}()
			}
			require.Equal(t, tt.dst, NamedRef(tt.name, tt.ref))
		})
	}
}

func TestNamedCastFailed(t *testing.T) {
	scanner := Named(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var A uint64
	err := scanner.ScanNamed(NamedRef("a", &A))
	require.ErrorIs(t, err, value.ErrCannotCast)
}

func TestNamedCastFailedErrMsg(t *testing.T) {
	scanner := Named(Data(
		[]*Ydb.Column{
			{
				Name: "a",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		},
		[]*Ydb.Value{
			{
				Value: &Ydb.Value_TextValue{
					TextValue: "test",
				},
			},
		},
	))
	var A uint64
	err := scanner.ScanNamed(NamedRef("a", &A))
	require.ErrorContains(t, err, "scan error on column name 'a': cast failed")
}
