package config

const (
	mainHelp = `Commands:
- create  - creates table in database
- cleanup - drops table in database
- run     - runs workload (read and write to table with sets RPS)
`
	createHelp = `Usage: slo-go-workload create <endpoint> <db> [options]

Arguments:
  endpoint                        YDB endpoint to connect to
  db                              YDB database to connect to

Options:
  -a <token>                      YDB access token credentials
  -t <tableName>                  table name to create
`
	cleanupHelp = `Usage: slo-go-workload cleanup <endpoint> <db> [options]

Arguments:
  endpoint                        YDB endpoint to connect to
  db                              YDB database to connect to

Options:
  -a <token>                      YDB access token credentials
  -t <tableName>                  table name to drop
`
	runHelp = `Usage: slo-go-workload run <endpoint> <db> [options]

Arguments:
  endpoint                        YDB endpoint to connect to
  db                              YDB database to connect to

Options:
  -a <token>                      YDB access token credentials
  -t <tableName>                  table name to read from
  --prom-pgw <promPgw>            prometheus push gateway
  --read-rps <readRps>            read RPS
  --read-timeout <readTimeout>    read timeout milliseconds
  --write-rps <writeRps>          write RPS
  --write-timeout <writeTimeout>  write timeout milliseconds
  --time <time>                   run time in seconds
  --shutdown-time <shutdownTime>  graceful shutdown time in seconds
  --report-period <reportPeriod>  prometheus push period in milliseconds
`
)
