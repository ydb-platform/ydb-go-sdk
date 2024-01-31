module github.com/ydb-platform/ydb-go-sdk/v3

go 1.20

require (
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/google/uuid v1.3.0
	github.com/jonboulle/clockwork v0.3.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20240126124512-dbb0e1720dbf
	golang.org/x/sync v0.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.57.1
	google.golang.org/protobuf v1.31.0
)

// requires for tests only
require (
	github.com/rekby/fixenv v0.3.2
	github.com/stretchr/testify v1.7.1
	go.uber.org/mock v0.3.1-0.20231011042131-892b665398ec // indirect
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230525234030-28d5490b6b19 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)
