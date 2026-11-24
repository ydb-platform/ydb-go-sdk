package topicclientinternal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func TestCommitOffsetErrors(t *testing.T) {
	withSessionID := []topicoptions.CommitOffsetOption{
		topicoptions.WithCommitOffsetReadSessionID("expired-session"),
	}
	expiredErr := xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
		xerrors.WithIssues([]*Ydb_Issue.IssueMessage{{
			IssueCode: 500004,
			Message:   "set offset in already dead session",
		}}),
	)
	expiredWithoutIssuesErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED))
	expiredConsumerErr := xerrors.Operation(
		xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
		xerrors.WithIssues([]*Ydb_Issue.IssueMessage{{
			IssueCode: 500004,
			Message:   "request to deleted read rule",
		}}),
	)
	badSessionErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
	badRequestErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST))

	for _, tt := range []struct {
		name      string
		opts      []topicoptions.CommitOffsetOption
		noRetry   bool
		results   []error
		wantErr   error
		wantCalls int
		wantRetry bool
	}{
		{
			name: "expired session", opts: withSessionID, results: []error{expiredErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name: "expired session without auto retry", opts: withSessionID, noRetry: true,
			results: []error{expiredWithoutIssuesErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name: "expired session with issues without auto retry", opts: withSessionID, noRetry: true,
			results: []error{expiredErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name:    "expired consumer without session ID retries",
			results: []error{expiredConsumerErr}, wantCalls: 2,
		},
		{
			name:    "explicitly empty session ID still retries",
			opts:    []topicoptions.CommitOffsetOption{topicoptions.WithCommitOffsetReadSessionID("")},
			results: []error{expiredErr}, wantCalls: 2,
		},
		{
			name: "without session ID or auto retry", noRetry: true,
			results: []error{expiredErr}, wantErr: expiredErr, wantCalls: 1, wantRetry: true,
		},
		{
			name: "bad session still retries", opts: withSessionID, results: []error{badSessionErr}, wantCalls: 2,
		},
		{
			name: "bad request stays unchanged", opts: withSessionID, results: []error{badRequestErr},
			wantErr: badRequestErr, wantCalls: 1,
		},
		{
			name: "expired session stops ongoing retries", opts: withSessionID,
			results: []error{badSessionErr, expiredErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			conn := mock.NewMockClientConnInterface(gomock.NewController(t))
			var calls int
			conn.EXPECT().Invoke(gomock.Any(), "/Ydb.Topic.V1.TopicService/CommitOffset", gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, _, reply any, _ ...grpc.CallOption) error {
					calls++
					if calls <= len(tt.results) {
						return tt.results[calls-1]
					}

					// Complete any extra attempt so a retry regression fails without hanging.
					reply.(*Ydb_Topic.CommitOffsetResponse).Operation = &Ydb_Operations.Operation{
						Ready: true, Status: Ydb.StatusIds_SUCCESS,
					}

					return nil
				}).AnyTimes()

			client := New(ctx, conn, nil, func(cfg *topic.Config) {
				config.SetAutoRetry(&cfg.Common, !tt.noRetry)
			})
			err := client.CommitOffset(ctx, "topic", 0, "consumer", 1, tt.opts...)
			require.Equal(t, tt.wantCalls, calls)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			originalErr := tt.results[len(tt.results)-1]
			require.ErrorIs(t, err, originalErr, "the original operation must remain accessible")
			require.Equal(t, xerrors.OperationError(originalErr), xerrors.OperationError(err))
			if !xerrors.Is(tt.wantErr, topicreader.ErrCommitToExpiredSession) {
				require.NotErrorIs(t, err, topicreader.ErrCommitToExpiredSession)
			}
			require.ErrorContains(t, err, originalErr.Error())
			require.Equal(t, tt.wantRetry, retry.Check(err).MustRetry(true))
		})
	}
}
