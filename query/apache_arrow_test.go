package query_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestWithResultFormatArrow(t *testing.T) {
	reader := &mockIPCReader{}
	readerOptions := []string{"reader option"}
	opt := query.WithResultFormatArrow(func(part io.Reader, opts ...string) (*mockIPCReader, error) {
		payload, err := io.ReadAll(part)
		require.NoError(t, err)
		require.Equal(t, "IPC part", string(payload))
		require.Equal(t, []string{"reader option"}, opts)

		return reader, nil
	}, readerOptions...)
	readerOptions[0] = "changed"
	settings := options.ExecuteSettings(opt)
	require.NotNil(t, settings.ArrowDecoder())
	batches, err := settings.ArrowDecoder()(t.Context(), nil, strings.NewReader("IPC part"))
	require.NoError(t, err)
	require.Empty(t, batches)
	require.Equal(t, 1, reader.releases)
	require.Nil(t, options.ExecuteSettings(opt, query.WithYdbValue()).ArrowDecoder())
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
