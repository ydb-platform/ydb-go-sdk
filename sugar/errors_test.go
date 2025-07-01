package sugar

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestPrintWithoutStack(t *testing.T) {
	for _, tt := range []struct {
		in  error
		out string
	}{
		{
			in:  errors.New("test"),
			out: "test",
		},
		{
			in:  xerrors.WithStackTrace(errors.New("test")),
			out: "test",
		},
		{
			in:  xerrors.WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "test")),
			out: "rpc error: code = Aborted desc = test",
		},
		{
			in: fmt.Errorf("test1: %w",
				xerrors.WithStackTrace(grpcStatus.Error(grpcCodes.Aborted, "test2")),
			),
			out: "test1: rpc error: code = Aborted desc = test2",
		},
		{
			in: fmt.Errorf("test1: %w",
				xerrors.WithStackTrace(errors.New("test2")),
			),
			out: "test1: test2",
		},
		{
			in: fmt.Errorf("test1: %w",
				xerrors.WithStackTrace(
					fmt.Errorf("test2: %w",
						xerrors.WithStackTrace(
							errors.New("test3"),
						),
					),
				),
			),
			out: "test1: test2: test3",
		},
	} {
		t.Run(tt.in.Error(), func(t *testing.T) {
			require.Equal(t, tt.out, PrintErrorWithoutStack(tt.in))
		})
	}
}

func TestRemoveStackRecords(t *testing.T) {
	require.Equal(t,
		"pool.With failed with 33 attempts: [\"attempt No.33: context deadline exceeded\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234645162 after several retries'}])\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234644725 after several retries'}])\"]", //nolint:lll
		removeStackRecords("pool.With failed with 33 attempts: [\"attempt No.33: context deadline exceeded\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234645162 after several retries'}]) at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.nextPart(result.go:221)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart(result.go:203)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult(result.go:166)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:138)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:149)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query(session.go:197)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do.func1(client.go:214)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try(pool.go:465)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With.func2(pool.go:493)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry.func1(retry.go:264)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.opWithRecover(retry.go:418)`\",\"query: operation/UNAVAILABLE (code = 400050, address = localhost:2135, issues = [{'Failed to resolve tablet: 72075186234644725 after several retries'}]) at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.nextPart(result.go:221)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart(result.go:203)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult(result.go:166)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:138)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:149)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query(session.go:197)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do.func1(client.go:214)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try(pool.go:465)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With.func2(pool.go:493)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry.func1(retry.go:264)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.opWithRecover(retry.go:418)`\"] at `github.com/ydb-platform/ydb-go-sdk/v3/retry.RetryWithResult(retry.go:374)` at `github.com/ydb-platform/ydb-go-sdk/v3/retry.Retry(retry.go:270)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With(pool.go:499)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do(client.go:222)`"), //nolint:lll
	)
}

func TestUnwrapError(t *testing.T) {
	for _, tt := range []struct {
		err  error
		errs []error
	}{
		{
			err:  errors.New("test1"),
			errs: []error{errors.New("test1")},
		},
		{
			err:  fmt.Errorf("%w", errors.New("test2")),
			errs: []error{errors.New("test2")},
		},
		{
			err: xerrors.Join(
				errors.New("test3_1"),
				errors.New("test3_2"),
			),
			errs: []error{
				errors.New("test3_1"),
				errors.New("test3_2"),
			},
		},
		{
			err: xerrors.Join(
				fmt.Errorf("%w", errors.New("test4_1")),
				fmt.Errorf("%w", errors.New("test4_2")),
			),
			errs: []error{
				errors.New("test4_1"),
				errors.New("test4_2"),
			},
		},
		{
			err:  xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test5"))),
			errs: []error{errors.New("test5")},
		},
		{
			err: xerrors.Join(
				xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test6_1"))),
				xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test6_2"))),
			),
			errs: []error{
				errors.New("test6_1"),
				errors.New("test6_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.Join(
					xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test7_1"))),
					xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test7_2"))),
				),
			),
			errs: []error{
				errors.New("test7_1"),
				errors.New("test7_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.WithStackTrace(
					xerrors.Join(
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test8_1"))),
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test8_2"))),
					),
				),
			),
			errs: []error{
				errors.New("test8_1"),
				errors.New("test8_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.WithStackTrace(
					xerrors.Join(
						xerrors.WithStackTrace(fmt.Errorf("%w",
							xerrors.WithStackTrace(
								errors.New("test9_1"),
							),
						)),
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test9_2"))),
					),
				),
			),
			errs: []error{
				errors.New("test9_1"),
				errors.New("test9_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.WithStackTrace(
					xerrors.Join(
						xerrors.WithStackTrace(fmt.Errorf("%w",
							xerrors.WithStackTrace(
								grpcStatus.Error(grpcCodes.Aborted, "test10_1"),
							),
						)),
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test10_2"))),
					),
				),
			),
			errs: []error{
				grpcStatus.Error(grpcCodes.Aborted, "test10_1"),
				errors.New("test10_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.WithStackTrace(
					xerrors.Join(
						xerrors.WithStackTrace(fmt.Errorf("%w",
							xerrors.WithStackTrace(
								xerrors.Transport(
									grpcStatus.Error(grpcCodes.Aborted, "test11_1"),
								),
							),
						)),
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test11_2"))),
					),
				),
			),
			errs: []error{
				grpcStatus.Error(grpcCodes.Aborted, "test11_1"),
				errors.New("test11_2"),
			},
		},
		{
			err: xerrors.WithStackTrace(
				xerrors.WithStackTrace(
					xerrors.Join(
						xerrors.WithStackTrace(fmt.Errorf("%w",
							xerrors.WithStackTrace(
								xerrors.Operation(
									xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
									xerrors.WithIssues([]*Ydb_Issue.IssueMessage{
										{
											Message: "test12_1",
										},
									}),
								),
							),
						)),
						xerrors.WithStackTrace(fmt.Errorf("%w", errors.New("test12_2"))),
					),
				),
			),
			errs: []error{
				xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
					xerrors.WithIssues([]*Ydb_Issue.IssueMessage{
						{
							Message: "test12_1",
						},
					}),
				),
				errors.New("test12_2"),
			},
		},
	} {
		t.Run(tt.err.Error(), func(t *testing.T) {
			require.Equal(t,
				fmt.Sprintf("%+v", tt.errs),
				fmt.Sprintf("%+v", UnwrapError(tt.err)),
			)
		})
	}
}
