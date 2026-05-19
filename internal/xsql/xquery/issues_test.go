package xquery

import (
	"context"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

func TestIssueHandlerContext(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		ih := IssuesHandlerFromContext(context.Background())
		if ih != nil {
			t.Fatal("expected nil issues handler option")
		}
	})

	t.Run("non-nil", func(t *testing.T) {
		var got []*Ydb_Issue.IssueMessage
		expected := []*Ydb_Issue.IssueMessage{
			{Message: "first"},
			{Message: "second"},
		}
		ctx := WithIssuesHandler(context.Background(), func(issues []*Ydb_Issue.IssueMessage) {
			got = issues
		})

		ih := IssuesHandlerFromContext(ctx)
		if ih == nil || ih.Callback == nil {
			t.Fatal("expected non-nil issues handler with non-nil callback")
		}

		ih.Callback(expected)

		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("expected issues: %#v, but got: %#v", expected, got)
		}
	})
}
