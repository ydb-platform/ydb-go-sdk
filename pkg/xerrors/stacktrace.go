package xerrors

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

func WithStackTrace(err error, opts ...xerrors.WithStackTraceOption) error {
	return xerrors.WithStackTrace(err, append(opts, WithSkipDepth(1))...)
}

func WithSkipDepth(skipDepth int) xerrors.WithStackTraceOption {
	return xerrors.WithSkipDepth(skipDepth)
}
