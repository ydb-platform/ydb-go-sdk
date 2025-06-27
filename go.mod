module github.com/ydb-platform/ydb-go-sdk/v3

go 1.22.5

require (
	github.com/golang-jwt/jwt/v4 v4.5.2
	github.com/google/uuid v1.6.0
	github.com/jonboulle/clockwork v0.5.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20241112172322-ea1f63298f77
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.0
	golang.org/x/sync v0.11.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.69.4
	google.golang.org/protobuf v1.35.1
)

// requires for tests only
require (
	github.com/rekby/fixenv v0.6.1
	github.com/stretchr/testify v1.10.0
	go.uber.org/mock v0.4.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v3.67.1 // decimal broken https://github.com/ydb-platform/ydb-go-sdk/issues/1234
