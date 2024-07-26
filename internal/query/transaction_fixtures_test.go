package query

import (
	"fmt"

	"github.com/rekby/fixenv"
)

func TransactionOverGrpcMock(e fixenv.Env) *Transaction {
	f := func() (*fixenv.GenericResult[*Transaction], error) {
		id := fmt.Sprintf("test-transaction-id-%v", e.T().Name())
		t := newTransaction(id, SessionOverGrpcMock(e))
		return fixenv.NewGenericResult(t), nil
	}
	return fixenv.CacheResult(e, f)
}
