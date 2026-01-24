package query

import (
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrMoreThanOneRow = errors.New("unexpected more than one row in result set")
	ErrNoRows         = &xerrors.ErrorWithCastToTargetError{
		Err:    errors.New("no rows in result set"),
		Target: io.EOF, // for compatibility with previously behaviour which returns internal error wrapped on io.EOF
	}
	ErrMoreThanOneResultSet = errors.New("unexpected more than one result set")
	ErrNoResultSets         = errors.New("no result sets")
)
