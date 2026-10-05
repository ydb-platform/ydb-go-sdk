package query_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestNewArrowDecoder(t *testing.T) {
	reader := &mockIPCReader{}
	decode := query.NewArrowDecoder(func(part io.Reader, opts ...string) (*mockIPCReader, error) {
		payload, err := io.ReadAll(part)
		require.NoError(t, err)
		require.Equal(t, "IPC part", string(payload))
		require.Equal(t, []string{"reader option"}, opts)

		return reader, nil
	}, "reader option")
	settings := options.ExecuteSettings(query.WithArrow(decode))
	require.NotNil(t, settings.ArrowDecoder())
	batches, err := settings.ArrowDecoder()(t.Context(), nil, strings.NewReader("IPC part"))
	require.NoError(t, err)
	require.Empty(t, batches)
	require.Equal(t, 1, reader.releases)
	require.Nil(t, options.ExecuteSettings(query.WithArrow(decode), query.WithArrow(nil)).ArrowDecoder())
}

type mockIPCReader struct{ releases int }

func (r *mockIPCReader) Read() (mockRecord, error) { return nil, io.EOF }
func (r *mockIPCReader) Release()                  { r.releases++ }

type mockRecord interface {
	Retain()
	Release()
	NumRows() int64
	NumCols() int64
	ColumnName(column int) string
	Column(column int) mockArray
}

type mockArray interface {
	Len() int
	IsNull(row int) bool
	NullN() int
}
