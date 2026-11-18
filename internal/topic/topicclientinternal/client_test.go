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
		name        string
		noRetry     bool
		noSessionID bool
		results     []error
		wantErr     error
		wantCalls   int
	}{
		{
			name: "expired session", results: []error{expiredErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name: "expired session without auto retry", noRetry: true,
			results: []error{expiredWithoutIssuesErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name: "expired consumer without session ID", noSessionID: true,
			results: []error{expiredConsumerErr},
			wantErr: topicreader.ErrCommitToExpiredSession, wantCalls: 1,
		},
		{
			name: "bad session still retries", results: []error{badSessionErr}, wantCalls: 2,
		},
		{
			name: "bad request stays unchanged", results: []error{badRequestErr},
			wantErr: badRequestErr, wantCalls: 1,
		},
		{
			name: "expired session stops ongoing retries", results: []error{badSessionErr, expiredErr},
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
			var opts []topicoptions.CommitOffsetOption
			if !tt.noSessionID {
				opts = append(opts, topicoptions.WithCommitOffsetReadSessionID("expired-session"))
			}
			err := client.CommitOffset(ctx, "topic", 0, "consumer", 1, opts...)
			require.Equal(t, tt.wantCalls, calls)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			if !xerrors.Is(tt.wantErr, topicreader.ErrCommitToExpiredSession) {
				require.NotErrorIs(t, err, topicreader.ErrCommitToExpiredSession)
			}
			require.ErrorContains(t, err, tt.results[len(tt.results)-1].Error())
			if tt.wantCalls == 1 {
				require.False(t, retry.Check(err).MustRetry(true))
			}
		})
	}
}
