package ydbsql

import (
	"bytes"
	"io"

	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
)

type Declaration struct {
	buf bytes.Buffer
}

func (d *Declaration) WriteTo(dest io.Writer) (int64, error) {
	return d.buf.WriteTo(dest)
}

func (d *Declaration) String() string {
	return d.buf.String()
}

func (d *Declaration) Declare(name string, t ydb.Type) {
	d.buf.WriteString("DECLARE $")
	d.buf.WriteString(name)
	d.buf.WriteString(" AS \"")
	internal.WriteTypeStringTo(&d.buf, t)
	d.buf.WriteString("\";\n")
}
