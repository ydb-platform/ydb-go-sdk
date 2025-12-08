package secret

import (
	"net/url"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

func DSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid DSN>"
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	if u.Scheme != "" {
		buffer.WriteString(u.Scheme + "://")
	}

	if u.User != nil {
		buffer.WriteString(u.User.Username())
		if password, has := u.User.Password(); has {
			buffer.WriteString(":" + Password(password))
		}
		buffer.WriteString("@")
	}

	buffer.WriteString(u.Host)
	buffer.WriteString(u.Path)

	if len(u.RawQuery) > 0 {
		buffer.WriteString("?")

		params := strings.Split(u.RawQuery, "&")

		for i, param := range params {
			if i > 0 {
				buffer.WriteString("&")
			}
			paramValue := strings.SplitN(param, "=", 2)
			buffer.WriteString(paramValue[0])
			if len(paramValue) > 1 {
				buffer.WriteString("=")
				switch paramValue[0] {
				case "token", "password":
					buffer.WriteString(Mask(paramValue[1]))
				default:
					buffer.WriteString(paramValue[1])
				}
			}
		}
	}

	if u.Fragment != "" {
		buffer.WriteString("#" + u.Fragment)
	}

	return buffer.String()
}
