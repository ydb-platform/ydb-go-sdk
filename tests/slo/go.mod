module slo

go 1.20

require (
	github.com/prometheus/client_golang v1.14.0
	github.com/ydb-platform/ydb-go-sdk-auth-environ v0.1.2
	github.com/ydb-platform/ydb-go-sdk-zap v0.15.0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.43.0
	go.uber.org/zap v1.24.0
	golang.org/x/sync v0.1.0
	golang.org/x/time v0.3.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20230403093326-123923969dc6 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20221215182650-986f9d10542f // indirect
	github.com/ydb-platform/ydb-go-yc v0.10.2 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.5.3 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20230403163135-c38d8f061ccd // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../../.
