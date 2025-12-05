package secret

import (
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

func DSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid DSN>"
	}

	values := u.Query()
	delete(values, "login")
	delete(values, "user")
	delete(values, "password")
	delete(values, "token")

	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString(u.Scheme + "://" + u.Host + u.Path)

	if len(values) > 0 {
		buffer.WriteString("?" + values.Encode())
	}

	return buffer.String()
}
