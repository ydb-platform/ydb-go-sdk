package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const expectedConsumedUnits = 2.0

// TestCommitConsumedUnitsAfterCanceledDrain checks that gRPC Canceled during result drain
// does not ban the connection and break stats on the next commit query.
//
// Scenario:
//  1. First commit stream: part 0 OK, then Canceled on drain (Close).
//  2. Second commit: ExecStats on stream part 0; client reads stats in WithStatsMode before Close.
//
// On master (v3.127.7+): streamWrapper bans the conn on Canceled => second commit may report zero units.
func TestCommitConsumedUnitsAfterCanceledDrain(t *testing.T) {
	mockSrv := mock.Server(t,
		mock.WithClusterNodes(1, 2, 3, 4),
		mock.WithCommitFirstCanceledThenStatsFirstPart(),
	)

	ctx := context.Background()
	db, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	var (
		firstCapacity  float64
		firstErr       error
		secondCapacity float64
	)
	err = db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		var commitErr error

		firstCapacity, firstErr = commitWithStatsBeforeClose(ctx, session)
		if firstErr != nil {
			return firstErr
		}
		require.EqualValues(t, 1, mockSrv.CommitQueryCalls(),
			"first commit must reach mock Canceled-on-drain path before second commit")

		secondCapacity, commitErr = commitWithStatsBeforeClose(ctx, session)

		return commitErr
	})
	require.NoError(t, err)
	require.NoError(t, firstErr,
		"first commit Query must succeed; mock Canceled applies on drain Close only")
	require.EqualValues(t, 0, firstCapacity,
		"first commit mock sends no ExecStats before Canceled")
	require.EqualValues(t, 2, mockSrv.CommitQueryCalls(),
		"second commit must run after first commit Canceled drain")
	require.Equal(t, expectedConsumedUnits, secondCapacity,
		"second commit after Canceled drain must report stats: 1 delete row × 2 write-unit multiplier = 2.0")
}

// commitWithStatsBeforeClose: commit via Query(WithCommit) and read stats before Close.
func commitWithStatsBeforeClose(ctx context.Context, session query.Session) (float64, error) {
	transaction, err := session.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
	if err != nil {
		return 0, err
	}

	var queryStats query.Stats
	res, err := transaction.Query(
		ctx,
		mock.CommitSelectOne,
		query.WithCommit(),
		query.WithStatsMode(query.StatsModeBasic, func(s query.Stats) {
			queryStats = s
		}),
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Close(ctx) }()

	return consumedUnitsFromStats(queryStats), nil
}

func consumedUnitsFromStats(st query.Stats) float64 {
	if st == nil {
		return 0
	}

	var readUnits, writeUnits uint64
	for phase, ok := st.NextPhase(); ok; phase, ok = st.NextPhase() {
		for access, ok := phase.NextTableAccess(); ok; access, ok = phase.NextTableAccess() {
			readUnits += readUnitsFromAccess(access.Reads.Rows, access.Reads.Bytes)
			writeUnits += writeUnitsFromAccess(access.Updates.Rows, access.Updates.Bytes)
			writeUnits += writeUnitsFromAccess(access.Deletes.Rows, access.Deletes.Bytes)
		}
	}

	return float64(readUnits + writeUnits*2)
}

func readUnitsFromAccess(rows, bytes uint64) uint64 {
	const bytesPerUnit = 4 * 1024
	units := roundUpUnits(bytes, bytesPerUnit)
	if units < rows {
		return rows
	}

	return units
}

func writeUnitsFromAccess(rows, bytes uint64) uint64 {
	const bytesPerUnit = 1024
	units := roundUpUnits(bytes, bytesPerUnit)
	if units < rows {
		return rows
	}

	return units
}

func roundUpUnits(v, unit uint64) uint64 {
	return (v + unit - 1) / unit
}
