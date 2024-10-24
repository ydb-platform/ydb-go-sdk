package metrics

import "time"

type Span struct {
	name  SpanName
	start time.Time
	m     *Metrics
}

type SpanName = string

const (
	OperationTypeRead  SpanName = "read"
	OperationTypeWrite SpanName = "write"
)

const (
	OperationStatusSuccess = "success"
	OperationStatusFailue  = "failure"
)
