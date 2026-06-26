package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// TestTableBulkUpsertIdempotentRetryOverride verifies that BulkUpsert prepends its
// default retry.WithIdempotent(true) before user options, so callers can override
// idempotency via table.WithRetryOptions(retry.WithIdempotent(false)).
//
// ABORTED is unconditionally retryable in the SDK retry policy (idempotent flag does
// not matter). UNDETERMINED is retried only for idempotent operations, so it is used
// to assert that WithIdempotent(false) suppresses retries.
func TestTableBulkUpsertIdempotentRetryOverride(t *testing.T) {
	const (
		failBeforeSuccess = 2
		wantCalls         = failBeforeSuccess + 1
	)

	noBackoff := backoff.New(
		backoff.WithSlotDuration(time.Millisecond),
		backoff.WithCeiling(1),
		backoff.WithJitterLimit(1),
	)

	fastRetry := table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
		retry.WithFastBackoff(noBackoff),
		retry.WithSlowBackoff(noBackoff),
	})

	data := table.BulkUpsertDataRows(types.ListValue(
		types.StructValue(
			types.StructFieldValue("id", types.Int64Value(1)),
		),
	))

	for _, tc := range []struct {
		name      string
		status    Ydb.StatusIds_StatusCode
		opts      []table.Option
		wantCalls uint64
		wantErr   bool
	}{
		{
			name:   "aborted_idempotent_true_retries",
			status: Ydb.StatusIds_ABORTED,
			opts: []table.Option{
				fastRetry,
				table.WithIdempotent(true),
			},
			wantCalls: wantCalls,
		},
		{
			name:   "aborted_idempotent_false_still_retries",
			status: Ydb.StatusIds_ABORTED,
			opts: []table.Option{
				fastRetry,
				table.WithIdempotent(false),
			},
			wantCalls: wantCalls,
		},
		{
			name:   "undetermined_idempotent_true_retries",
			status: Ydb.StatusIds_UNDETERMINED,
			opts: []table.Option{
				fastRetry,
				table.WithIdempotent(true),
			},
			wantCalls: wantCalls,
		},
		{
			name:   "undetermined_idempotent_false_no_retry",
			status: Ydb.StatusIds_UNDETERMINED,
			opts: []table.Option{
				fastRetry,
				table.WithIdempotent(false),
			},
			wantCalls: 1,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockSrv := mock.Server(t, mock.WithBulkUpsertFailFirst(failBeforeSuccess, tc.status))

			ctx := context.Background()

			driver, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = driver.Close(ctx)
			})

			err = driver.Table().BulkUpsert(ctx, "test", data, tc.opts...)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantCalls, mockSrv.BulkUpsertCalls())
		})
	}
}
