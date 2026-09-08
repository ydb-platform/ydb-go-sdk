module github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts

go 1.24.0

require (
	github.com/cucumber/godog v0.16.0
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20260810122915-65bfd5c4b705
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/cucumber/gherkin/go/v42 v42.0.0 // indirect
	github.com/cucumber/messages/go/v34 v34.2.0 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../..
