all: basic bulk_upsert containers ddl decimal serverless/healthcheck pagination partitioning_policies read_table ttl ttl_readtable serverless/url_shortener

lint:
	golangci-lint run ./basic ./bulk_upsert ./containers ./ddl ./decimal ./healthcheck ./pagination ./partitioning_policies ./read_table ./ttl ./ttl_readtable ./url_shortener

basic:
	go run ./basic/native -ydb=${YDB_CONNECTION_STRING} -prefix=basic

database_sql:
	go run ./basic/database_sql -ydb=${YDB_CONNECTION_STRING} -prefix=database/sql

bulk_upsert:
	go run ./bulk_upsert -ydb=${YDB_CONNECTION_STRING} -prefix=bulk_upsert -table=bulk_upsert

containers:
	go run ./containers -ydb=${YDB_CONNECTION_STRING} -prefix=containers

ddl:
	go run ./ddl -ydb=${YDB_CONNECTION_STRING} -prefix=ddl

decimal:
	go run ./decimal -ydb=${YDB_CONNECTION_STRING} -prefix=decimal

pagination:
	go run ./pagination -ydb=${YDB_CONNECTION_STRING} -prefix=pagination

partitioning_policies:
	go run ./partitioning_policies -ydb=${YDB_CONNECTION_STRING} -prefix=partitioning_policies -table=partitioning_policies

read_table:
	go run ./read_table -ydb=${YDB_CONNECTION_STRING} -prefix=read_table

ttl:
	go run ./ttl -ydb=${YDB_CONNECTION_STRING} -prefix=ttl

ttl_readtable:
	go run ./ttl_readtable -ydb=${YDB_CONNECTION_STRING} -prefix=ttl_readtable

healthcheck:
	go run ./serverless/healthcheck -ydb=${YDB_CONNECTION_STRING} -prefix=healthcheck -url=ya.ru -url=google.com

url_shortener:
	go run ./serverless/url_shortener -ydb=${YDB_CONNECTION_STRING} -prefix=url_shortener

describe:
	go run ./describe -ydb=${YDB_CONNECTION_STRING} -prefix=/ -t="Optional<Interval>" -t="Interval"
