package query

import (
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrMoreThanOneRow = errors.New("unexpected more than one row in result set")
	ErrNoRows         = xerrors.IsTarget(
		errors.New("no rows in result set"),
		io.EOF, // for compatibility with previously behavior which returns internal error wrapped on io.EOF
	)
	ErrMoreThanOneResultSet   = errors.New("unexpected more than one result set")
	ErrNoResultSets           = errors.New("no result sets")
	ErrTxControlWithoutCommit = errors.New("read-write transaction control with BeginTx requires CommitTx to be set, otherwise data changes will not be committed to the database")
)
