export GOOS=darwin
export GOARCH=arm64
export CGO_ENABLED=0

.bin/xorm_${GOOS}_${GOARCH} create  grpc://localhost:2135 /Root/testdb
.bin/xorm_${GOOS}_${GOARCH} run     grpc://localhost:2135 /Root/testdb -prom-pgw localhost:9091 -report-period 500 -read-rps 100 -write-rps 10 -time 60
.bin/xorm_${GOOS}_${GOARCH} cleanup grpc://localhost:2135 /Root/testdb

sleep 10

.bin/gorm_${GOOS}_${GOARCH} create  grpc://localhost:2135 /Root/testdb
.bin/gorm_${GOOS}_${GOARCH} run     grpc://localhost:2135 /Root/testdb -prom-pgw localhost:9091 -report-period 500 -time 60
.bin/gorm_${GOOS}_${GOARCH} cleanup grpc://localhost:2135 /Root/testdb

sleep 10

.bin/database_sql_${GOOS}_${GOARCH} create  grpc://localhost:2135 /Root/testdb
.bin/database_sql_${GOOS}_${GOARCH} run     grpc://localhost:2135 /Root/testdb -prom-pgw localhost:9091 -report-period 500 -time 60
.bin/database_sql_${GOOS}_${GOARCH} cleanup grpc://localhost:2135 /Root/testdb

sleep 10

.bin/native_table_${GOOS}_${GOARCH} create  grpc://localhost:2135 /Root/testdb
.bin/native_table_${GOOS}_${GOARCH} run     grpc://localhost:2135 /Root/testdb -prom-pgw localhost:9091 -report-period 500 -time 60
.bin/native_table_${GOOS}_${GOARCH} cleanup grpc://localhost:2135 /Root/testdb

sleep 10

.bin/native_query_${GOOS}_${GOARCH} create  grpc://localhost:2135 /Root/testdb
.bin/native_query_${GOOS}_${GOARCH} run     grpc://localhost:2135 /Root/testdb -prom-pgw localhost:9091 -report-period 500 -time 60
.bin/native_query_${GOOS}_${GOARCH} cleanup grpc://localhost:2135 /Root/testdb
