module github.com/ydb-platform/ydb-go-sdk/v3

go 1.20

require (
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/google/uuid v1.6.0
	github.com/jonboulle/clockwork v0.3.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20240528144234-5d5a685e41f7
	golang.org/x/net v0.23.0
	golang.org/x/sync v0.6.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.62.1
	google.golang.org/protobuf v1.33.0
)

// requires for tests only
require (
	github.com/rekby/fixenv v0.6.1
	github.com/stretchr/testify v1.7.1
	go.uber.org/mock v0.4.0
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240123012728-ef4313101c80 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)

retract v3.67.1 // decimal broken https://github.com/ydb-platform/ydb-go-sdk/issues/1234
