package wrap

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// StreamOperationResponse is an interface that provides access to the
// API-specific response fields.
//
// NOTE: YDB API currently does not provide generic response wrapper as it does
// with RPC API. Thus wee need to generalize it by the hand using this interface.
//
// This generalization is needed for checking status codes and issues in one place.
type StreamOperationResponse interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}
