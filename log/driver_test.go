package log

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDriverLogsYQLIssueWithChildren(t *testing.T) {
	logger := new(recordingLogger)
	driverTrace := Driver(logger, trace.DriverConnEvents)
	onDone := driverTrace.OnConnInvoke(trace.DriverConnInvokeStartInfo{
		Method: "/Ydb.Table.V1.TableService/ExecuteDataQuery",
	})

	onDone(trace.DriverConnInvokeDoneInfo{
		Issues: []trace.Issue{
			&Ydb_Issue.IssueMessage{
				Message:   "Type annotation",
				IssueCode: 1030,
				Severity:  2,
				Issues: []*Ydb_Issue.IssueMessage{
					{
						Message:   "Failed to convert type",
						IssueCode: 2030,
						Severity:  1,
					},
				},
			},
		},
	})

	records := logger.recordsWithMessage("YQL issue")
	require.Len(t, records, 1)
	require.Equal(t, "/Ydb.Table.V1.TableService/ExecuteDataQuery", fieldValue(records[0].fields, "method"))
	require.Equal(t, "", fieldValue(records[0].fields, "endpoint"))

	issueJSON, err := json.Marshal(fieldValue(records[0].fields, "issue"))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"message": "Type annotation",
		"code": 1030,
		"severity": 2,
		"issues": [{
			"message": "Failed to convert type",
			"code": 2030,
			"severity": 1
		}]
	}`, string(issueJSON))
	require.Len(t, records[0].fields, 3)
}

func TestMakeIssueLogLimit(t *testing.T) {
	children := make([]*Ydb_Issue.IssueMessage, maxIssueLogEntries)
	for i := range children {
		children[i] = &Ydb_Issue.IssueMessage{Message: "child"}
	}

	actual := makeIssueLog(&Ydb_Issue.IssueMessage{
		Message: "root",
		Issues:  children,
	})

	require.Len(t, actual.Issues, maxIssueLogEntries)
	require.Equal(t, "child", actual.Issues[maxIssueLogEntries-2].Message)
	require.Equal(t, issueLog{
		Message:   moreIssuesMessage,
		Truncated: true,
	}, actual.Issues[maxIssueLogEntries-1])
}

type logRecord struct {
	message string
	fields  []Field
}

type recordingLogger struct {
	records []logRecord
}

func (l *recordingLogger) Log(_ context.Context, message string, fields ...Field) {
	l.records = append(l.records, logRecord{
		message: message,
		fields:  append([]Field(nil), fields...),
	})
}

func (l *recordingLogger) recordsWithMessage(message string) (records []logRecord) {
	for _, record := range l.records {
		if record.message == message {
			records = append(records, record)
		}
	}

	return records
}

func fieldValue(fields []Field, key string) any {
	for _, field := range fields {
		if field.Key() == key {
			return field.AnyValue()
		}
	}

	return nil
}
