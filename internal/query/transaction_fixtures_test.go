package query

import (
	"fmt"

	"github.com/rekby/fixenv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

func TransactionOverGrpcMock(e fixenv.Env) *Transaction {
	f := func() (*fixenv.GenericResult[*Transaction], error) {
		return fixenv.NewGenericResult(&Transaction{
			LazyID: func() (id tx.LazyID) {
				id.SetTxID(fmt.Sprintf("test-transaction-id-%v", e.T().Name()))

				return id
			}(),
			s: SessionOverGrpcMock(e),
		}), nil
	}

	return fixenv.CacheResult(e, f)
}
