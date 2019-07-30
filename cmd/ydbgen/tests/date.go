package tests

import "time"

//go:generate ydbgen

//ydb:gen
type Times struct {
	Date time.Time `ydb:"type:Date?"`
	//Datetime    time.Time `ydb:"type:Datetime"`
	//Timestamp   time.Time `ydb:"type:Timestamp"`
	//Interval    time.Time `ydb:"type:Interval"`
	//TzDate      time.Time `ydb:"type:TzDate"`
	//TzDatetime  time.Time `ydb:"type:TzDatetime"`
	//TzTimestamp time.Time `ydb:"type:TzTimestamp"`
}
