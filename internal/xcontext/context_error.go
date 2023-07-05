package xcontext

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

var _ error = (*ctxErr)(nil)

const (
	atWord   = "at"
	fromWord = "from"
)

func errAt(err error, skipDepth int) error {
	return &ctxErr{
		err:         err,
		stackRecord: stack.Record(skipDepth + 1),
		linkWord:    atWord,
	}
}

func errFrom(err error, from string) error {
	return &ctxErr{
		err:         err,
		stackRecord: from,
		linkWord:    fromWord,
	}
}

type ctxErr struct {
	err         error
	stackRecord string
	linkWord    string
}

func (e *ctxErr) Error() string {
	return "'" + e.err.Error() + "' " + e.linkWord + " `" + e.stackRecord + "`"
}

func (e *ctxErr) Unwrap() error {
	return e.err
}
