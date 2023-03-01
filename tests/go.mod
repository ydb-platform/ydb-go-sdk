module github.com/ydb-platform/ydb-go-sdk/v3/tests

go 1.19

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../

require (
	github.com/stretchr/testify v1.8.2
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20221215182650-986f9d10542f
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-00010101000000-000000000000
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.5.0 // indirect
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.6.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
