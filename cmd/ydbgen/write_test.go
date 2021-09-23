package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"testing"
)

func TestGenerator_importDeps(t *testing.T) {
	g := Generator{}
	var buf bytes.Buffer

	bw := bufio.NewWriter(&buf)
	g.importDeps(bw)
	err := bw.Flush()
	if err != nil {
		t.Fatal(fmt.Sprintf("Received unexpected error:\n%+v", err))
	}

	expected := `import (
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	_ = strconv.Itoa
	_ = ydb.StringValue
	_ = table.NewQueryParameters
)

`
	assert.Equal(t, expected, buf.String())
}
