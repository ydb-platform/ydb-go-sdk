package tests

import "time"

//ydb:gen
type Times struct {
	Date time.Time `ydb:"types:Date?"`
	//Datetime    time.Time `ydb:"types:Datetime"`
	//Timestamp   time.Time `ydb:"types:Timestamp"`
	//Interval    time.Time `ydb:"types:Interval"`
	//TzDate      time.Time `ydb:"types:TzDate"`
	//TzDatetime  time.Time `ydb:"types:TzDatetime"`
	//TzTimestamp time.Time `ydb:"types:TzTimestamp"`
}
