package test

import "os"

func CheckEndpointDatabaseEnv() bool {
	_, okEndpoint := os.LookupEnv("YDB_ENDPOINT")
	_, okDatabase := os.LookupEnv("YDB_DATABASE")
	return okEndpoint && okDatabase
}
