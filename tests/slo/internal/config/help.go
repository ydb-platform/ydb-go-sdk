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
  -t -table-name         <string> table name to create

  -min-partitions-count  <int>    minimum amount of partitions in table
  -max-partitions-count  <int>    maximum amount of partitions in table
  -partition-size        <int>    partition size in mb
                                   
  -c -initial-data-count <int>    amount of initially created rows
                                   
  -write-timeout         <int>    write timeout milliseconds
`
	cleanupHelp = `Usage: slo-go-workload cleanup <endpoint> <db> [options]

Arguments:
  endpoint                        YDB endpoint to connect to
  db                              YDB database to connect to

Options:
  -t -table-name         <string> table name to create
                         
  -write-timeout         <int>    write timeout milliseconds
`
	runHelp = `Usage: slo-go-workload run <endpoint> <db> [options]

Arguments:
  endpoint                        YDB endpoint to connect to
  db                              YDB database to connect to

Options:
  -t -table-name         <string> table name to create
                         
  -initial-data-count    <int>    amount of initially created rows
                         
  -prom-pgw              <string> prometheus push gateway
  -report-period         <int>    prometheus push period in milliseconds
                         
  -read-rps              <int>    read RPS
  -read-timeout          <int>    read timeout milliseconds
                         
  -write-rps             <int>    write RPS
  -write-timeout         <int>    write timeout milliseconds
                         
  -time                  <int>    run time in seconds
  -shutdown-time         <int>    graceful shutdown time in seconds
`
)
