export REF=$(git symbolic-ref --short HEAD)

export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=0

go build -o .bin/gorm_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=gorm\" -X \"main.jobName=gorm\"" ./gorm
go build -o .bin/xorm_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=xorm\" -X \"main.jobName=xorm\"" ./xorm
go build -o .bin/database_sql_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=database/sql\" -X \"main.jobName=database-sql\"" ./database/sql
go build -o .bin/native_query_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=native/query\" -X \"main.jobName=native-query\"" ./native/query
go build -o .bin/native_table_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=native/table\" -X \"main.jobName=native-table\"" ./native/table

export GOOS=darwin
export GOARCH=arm64
export CGO_ENABLED=0

go build -o .bin/gorm_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=gorm\" -X \"main.jobName=gorm\"" ./gorm
go build -o .bin/xorm_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=xorm\" -X \"main.jobName=xorm\"" ./xorm
go build -o .bin/database_sql_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=database/sql\" -X \"main.jobName=database-sql\"" ./database/sql
go build -o .bin/native_query_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=native/query\" -X \"main.jobName=native-query\"" ./native/query
go build -o .bin/native_table_${GOOS}_${GOARCH} -ldflags "-X \"main.ref=${REF}\" -X \"main.label=native/table\" -X \"main.jobName=native-table\"" ./native/table
