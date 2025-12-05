package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDSN(t *testing.T) {
	for _, tt := range []struct {
		src string
		res string
	}{
		{
			src: "not-a-valid-url",
			res: "<invalid DSN>",
		},
		{
			src: "grpc://debuguser:debugpassword@localhost:2136/local1",
			res: "grpc://localhost:2136/local1",
		},
		{
			src: "grpc://localhost:2136/local1?user=debuguser&password=debugpassword",
			res: "grpc://localhost:2136/local1",
		},
		{
			src: "grpc://localhost:2136/local1?login=debuguser&password=debugpassword",
			res: "grpc://localhost:2136/local1",
		},
		{
			src: "grpc://localhost:2136/local1?param1=value1&login=debuguser&param2=value2&password=debugpassword&param2=value3",
			res: "grpc://localhost:2136/local1?param1=value1&param2=value2&param2=value3",
		},
		{
			src: "grpc://localhost:2136/local1?token=secrettoken123",
			res: "grpc://localhost:2136/local1",
		},
	} {
		t.Run(tt.src, func(t *testing.T) {
			require.Equal(t, tt.res, DSN(tt.src))
		})
	}
}
