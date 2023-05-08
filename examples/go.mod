module examples

go 1.18

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/prometheus/client_golang v1.13.0
	github.com/rs/zerolog v1.27.0
	github.com/ydb-platform/gorm-driver v0.0.4
	github.com/ydb-platform/ydb-go-sdk-auth-environ v0.1.2
	github.com/ydb-platform/ydb-go-sdk-prometheus v0.11.10
	github.com/ydb-platform/ydb-go-sdk-zerolog v0.12.2
	github.com/ydb-platform/ydb-go-sdk/v3 v3.44.0
	github.com/ydb-platform/ydb-go-yc v0.10.1
	google.golang.org/genproto v0.0.0-20220822174746-9e6da59bd2fc
	gorm.io/driver/postgres v1.4.6
	gorm.io/driver/sqlite v1.4.4
	gorm.io/gorm v1.24.5
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.2.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mattn/go-sqlite3 v1.14.15 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/yandex-cloud/go-genproto v0.0.0-20220815090733-4c139c0154e2 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20221215182650-986f9d10542f // indirect
	github.com/ydb-platform/ydb-go-sdk-metrics v0.16.3 // indirect
	github.com/ydb-platform/ydb-go-yc-metadata v0.5.4 // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sync v0.0.0-20220923202941-7f9b1623fab7 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/grpc v1.49.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../
