package test

import "os"

func CheckEndpointDatabaseEnv() bool {
	_, ok := os.LookupEnv("YDB_CONNECTION_STRING")
	return ok
}
