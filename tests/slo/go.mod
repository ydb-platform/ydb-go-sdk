module slo

go 1.20

require (
	github.com/prometheus/client_golang v1.14.0
	github.com/ydb-platform/gorm-driver v0.1.1
	github.com/ydb-platform/ydb-go-sdk/v3 v3.49.0
	golang.org/x/sync v0.3.0
	golang.org/x/time v0.3.0
	gorm.io/gorm v1.25.1
	xorm.io/xorm v1.3.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20230403093326-123923969dc6 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20231012155159-f85a672542fd // indirect
	github.com/ydb-platform/ydb-go-sdk-auth-environ v0.2.0 // indirect
	github.com/ydb-platform/ydb-go-yc v0.10.2 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.5.3 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230526161137-0005af68ea54 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230525234035-dd9d682886f9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230525234030-28d5490b6b19 // indirect
	google.golang.org/grpc v1.57.1 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	modernc.org/sqlite v1.24.0 // indirect
	xorm.io/builder v0.3.11-0.20220531020008-1bd24a7dc978 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../../.

replace xorm.io/xorm => github.com/ydb-platform/xorm v0.0.6
