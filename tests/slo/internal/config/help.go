package config

const (
	mainHelp = `Commands:
- create  - creates table in database
- cleanup - drops table in database
- run     - runs workload (read and write to table with sets RPS)
`
	createHelp = `Usage: slo-go-workload create <endpoint> <db> [options]

Arguments:
  endpoint                      YDB endpoint to connect to
  db                            YDB database to connect to

Options:
  -a                  <string>  YDB access token credentials
  -t                  <string>  table name to create
  -partitions-count   <int>     amount of partitions in table creation
  -initial-data-count <int>     amount of initially created rows
`
	cleanupHelp = `Usage: slo-go-workload cleanup <endpoint> <db> [options]

Arguments:
  endpoint                      YDB endpoint to connect to
  db                            YDB database to connect to

Options:
  -a <string>                   YDB access token credentials
  -t <string>                   table name to drop
`
	runHelp = `Usage: slo-go-workload run <endpoint> <db> [options]

Arguments:
  endpoint                      YDB endpoint to connect to
  db                            YDB database to connect to

Options:
  -a              <string>      YDB access token credentials
  -t              <string>      table name
  --prom-pgw      <string>      prometheus push gateway
  --read-rps      <int>         read RPS
  --read-timeout  <int>         read timeout milliseconds
  --write-rps     <int>         write RPS
  --write-timeout <int>         write timeout milliseconds
  --time          <int>         run time in seconds
  --shutdown-time <int>         graceful shutdown time in seconds
  --report-period <int>         prometheus push period in milliseconds
`
)
