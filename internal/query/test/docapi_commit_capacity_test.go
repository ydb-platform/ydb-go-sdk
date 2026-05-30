package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const commitQuery = "select 1"

const docapiExpectedCapacity = 2.0

// TestDocAPICommitCapacityAfterCanceledDrain reproduces docapi delete billing after a poison commit.
//
// Scenario:
//  1. First commit stream returns Canceled on drain (delete Close) — must not ban the connection.
//  2. Second commit returns ExecStats on stream part 0; docapi reads stats before Close (like ExecInQueryTransaction).
//
// Regression: streamWrapper bans conn on gRPC Canceled (#2040) => flaky CapacityUnits == 0 on the billing commit.
func TestDocAPICommitCapacityAfterCanceledDrain(t *testing.T) {
	mockSrv := mock.Server(t,
		mock.WithClusterNodes(1, 2, 3, 4),
		mock.WithCommitFirstCanceledThenStatsFirstPart(),
	)

	ctx := context.Background()
	db, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	var capacity float64
	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		_, _ = docapiCommitBeforeClose(ctx, session)

		var commitErr error
		capacity, commitErr = docapiCommitBeforeClose(ctx, session)

		return commitErr
	})
	require.NoError(t, err)
	require.Equal(t, docapiExpectedCapacity, capacity,
		"docapi DeleteItem expectRelevant(1, 2): billing commit after Canceled drain must keep capacity")
}

// docapiCommitBeforeClose mirrors serverless/database/docapi/request/exec/transaction.go commit path.
func docapiCommitBeforeClose(ctx context.Context, session query.Session) (float64, error) {
	transaction, err := session.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
	if err != nil {
		return 0, err
	}

	var queryStats query.Stats
	res, err := transaction.Query(
		ctx,
		commitQuery,
		query.WithCommit(),
		query.WithStatsMode(query.StatsModeBasic, func(s query.Stats) {
			queryStats = s
		}),
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Close(ctx) }()

	return docapiCapacityUnits(queryStats), nil
}

func docapiCapacityUnits(st query.Stats) float64 {
	if st == nil {
		return 0
	}

	var readUnits, writeUnits uint64
	for phase, ok := st.NextPhase(); ok; phase, ok = st.NextPhase() {
		for access, ok := phase.NextTableAccess(); ok; access, ok = phase.NextTableAccess() {
			readUnits += docapiReadUnits(access.Reads.Rows, access.Reads.Bytes)
			writeUnits += docapiWriteUnits(access.Updates.Rows, access.Updates.Bytes)
			writeUnits += docapiWriteUnits(access.Deletes.Rows, access.Deletes.Bytes)
		}
	}

	return float64(readUnits + writeUnits*2)
}

func docapiReadUnits(rows, bytes uint64) uint64 {
	const bytesPerUnit = 4 * 1024
	units := docapiRoundUp(bytes, bytesPerUnit)
	if units < rows {
		return rows
	}

	return units
}

func docapiWriteUnits(rows, bytes uint64) uint64 {
	const bytesPerUnit = 1024
	units := docapiRoundUp(bytes, bytesPerUnit)
	if units < rows {
		return rows
	}

	return units
}

func docapiRoundUp(v, unit uint64) uint64 {
	return (v + unit - 1) / unit
}
