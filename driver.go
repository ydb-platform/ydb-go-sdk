package ydb

import (
	"time"
)

type driver struct {
	cluster *cluster
	meta    *meta
	trace   DriverTrace

	requestTimeout       time.Duration
	streamTimeout        time.Duration
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
}
