module github.com/ydb-platform/ydb-go-sdk/v3

go 1.20

require (
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/google/uuid v1.3.0
	github.com/jonboulle/clockwork v0.3.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20231012155159-f85a672542fd
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.28.1
)

// requires for tests only
require (
	github.com/golang/mock v1.6.0
	github.com/rekby/fixenv v0.3.2
	github.com/stretchr/testify v1.7.1
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)
