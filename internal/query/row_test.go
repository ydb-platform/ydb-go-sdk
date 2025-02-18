package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
)

type testScanner struct {
	err error
}

func (s testScanner) Scan(dst ...interface{}) error {
	return s.err
}

func (s testScanner) ScanNamed(dst ...scanner.NamedDestination) error {
	return s.err
}

func (s testScanner) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) error {
	return s.err
}

func TestRowScan(t *testing.T) {
	expErr := errors.New("test error")
	row := Row{
		indexedScanner: testScanner{err: expErr},
		namedScanner:   testScanner{err: expErr},
		structScanner:  testScanner{err: expErr},
	}
	for _, tt := range []struct {
		name      string
		scan      func() error
		expErrStr string
	}{
		{
			name: "indexed scan",
			scan: func() error {
				return row.Scan()
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.Scan(row.go:39)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func1(row_test.go:43)`", //nolint:lll
		},
		{
			name: "named scan",
			scan: func() error {
				return row.ScanNamed()
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanNamed(row.go:51)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func2(row_test.go:50)`", //nolint:lll
		},
		{
			name: "struct scan",
			scan: func() error {
				return row.ScanStruct(nil)
			},
			expErrStr: "test error at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.Row.ScanStruct(row.go:63)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.TestRowScan.func3(row_test.go:57)`", //nolint:lll
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scan()
			require.Error(t, err)
			require.ErrorIs(t, err, expErr)
			require.Equal(t, tt.expErrStr, err.Error())
		})
	}
}
