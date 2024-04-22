package retry

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// ErrNoQuota is a special error for no quota provided by external retry budget
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
var ErrNoQuota = xerrors.Wrap(errors.New("no retry quota"))

type budget interface {
	retry.Budget
	Stop()
}

func Budget(attemptsPerSecond int, opts ...retry.BudgetOption) budget {
	return retry.NewBudget(attemptsPerSecond, opts...)
}
