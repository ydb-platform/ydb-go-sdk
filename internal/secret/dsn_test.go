package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDSN(t *testing.T) {
	for _, tt := range []struct {
		dsn string
		exp string
	}{
		{
			dsn: "grpc://192.168.0.%31:2136/",
			exp: "<invalid DSN>",
		},
		{
			dsn: "grpc://debuguser:debugpassword@localhost:2136/local1",
			exp: "grpc://debuguser:d***********d@localhost:2136/local1",
		},
		{
			dsn: "grpc://localhost:2136/local1?user=debuguser&password=debugpassword",
			exp: "grpc://localhost:2136/local1?user=debuguser&password=d***********d",
		},
		{
			dsn: "grpc://localhost:2136/local1?login=debuguser&password=debugpassword",
			exp: "grpc://localhost:2136/local1?login=debuguser&password=d***********d",
		},
		{
			dsn: "grpc://localhost:2136/local1?param1=value1&login=debuguser&param2=value2&password=debugpassword&param2=value3",
			exp: "grpc://localhost:2136/local1?param1=value1&login=debuguser&param2=value2&password=d***********d&param2=value3",
		},
		{
			dsn: "grpc://localhost:2136/local1?param1&login=debuguser&param2=value2&password=debugpassword&param2=value3",
			exp: "grpc://localhost:2136/local1?param1&login=debuguser&param2=value2&password=d***********d&param2=value3",
		},
		{
			dsn: "grpc://localhost:2136/local1?token=secrettoken123",
			exp: "grpc://localhost:2136/local1?token=s************3",
		},
	} {
		t.Run(tt.dsn, func(t *testing.T) {
			require.Equal(t, tt.exp, DSN(tt.dsn))
		})
	}
}
