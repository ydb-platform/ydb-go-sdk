package query

import (
	"fmt"

	"github.com/rekby/fixenv"
)

func TransactionOverGrpcMock(e fixenv.Env) *transaction {
	f := func() (*fixenv.GenericResult[*transaction], error) {
		id := fmt.Sprintf("test-transaction-id-%v", e.T().Name())
		t := newTransaction(id, SessionOverGrpcMock(e))

		return fixenv.NewGenericResult(t), nil
	}

	return fixenv.CacheResult(e, f)
}
