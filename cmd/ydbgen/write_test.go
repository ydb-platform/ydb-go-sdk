package main

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerator_importDeps(t *testing.T) {
	g := Generator{}
	var buf bytes.Buffer

	bw := bufio.NewWriter(&buf)
	g.importDeps(bw)
	require.NoError(t, bw.Flush())
	require.Equal(
		t,
		`import (
	"strconv"

	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"github.com/YandexDatabase/ydb-go-sdk/v3/table"
)

var (
	_ = strconv.Itoa
	_ = ydb.StringValue
	_ = table.NewQueryParameters
)

`,
		buf.String(),
	)
}
