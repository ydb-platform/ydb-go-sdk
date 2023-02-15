package rawtopiccommon

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type StatusAndIssues interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

type ServerMessageMetadata struct {
	Status rawydb.StatusCode
	Issues rawydb.Issues
}

func (m *ServerMessageMetadata) MetaFromStatusAndIssues(p StatusAndIssues) error {
	if err := m.Status.FromProto(p.GetStatus()); err != nil {
		return err
	}

	return m.Issues.FromProto(p.GetIssues())
}

func (m *ServerMessageMetadata) StatusData() ServerMessageMetadata {
	return *m
}

func (m *ServerMessageMetadata) SetStatus(status rawydb.StatusCode) {
	m.Status = status
}
