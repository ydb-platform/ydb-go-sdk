module github.com/ydb-platform/ydb-go-sdk/v3

go 1.16

require (
	github.com/ydb-platform/ydb-api-protos v0.0.0-20210921091122-f697ac767e19 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20210916081217-f4e55570b874
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)

retract (
	v3.0.0
	v3.0.1
	v3.0.2
	v3.0.3
)