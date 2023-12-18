module examples

go 1.20

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/lib/pq v1.10.2
	github.com/prometheus/client_golang v1.13.0
	github.com/ydb-platform/gorm-driver v0.0.5
	github.com/ydb-platform/ydb-go-sdk-auth-environ v0.1.2
	github.com/ydb-platform/ydb-go-sdk-prometheus v0.11.10
	github.com/ydb-platform/ydb-go-sdk/v3 v3.47.3
	github.com/ydb-platform/ydb-go-yc v0.10.1
	google.golang.org/genproto v0.0.0-20230526161137-0005af68ea54
	gorm.io/driver/postgres v1.5.0
	gorm.io/driver/sqlite v1.5.0
	gorm.io/gorm v1.25.1
	modernc.org/sqlite v1.18.1
	xorm.io/builder v0.3.11-0.20220531020008-1bd24a7dc978
	xorm.io/xorm v1.3.2
)

require (
	cloud.google.com/go/firestore v1.9.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.4.3 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-sqlite3 v1.14.15 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20220815090733-4c139c0154e2 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20231215113745-46f6d30f974a // indirect
	github.com/ydb-platform/ydb-go-sdk-metrics v0.16.3 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.5.4 // indirect
	golang.org/x/crypto v0.13.0 // indirect
	golang.org/x/mod v0.9.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/tools v0.7.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230525234035-dd9d682886f9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230525234030-28d5490b6b19 // indirect
	google.golang.org/grpc v1.57.1 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.36.3 // indirect
	modernc.org/ccgo/v3 v3.16.9 // indirect
	modernc.org/libc v1.17.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.2.1 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.0 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../

replace xorm.io/xorm => github.com/ydb-platform/xorm v0.0.3
