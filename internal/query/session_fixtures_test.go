package query

import (
	"fmt"

	"github.com/rekby/fixenv"
	"go.uber.org/mock/gomock"
)

func SessionOverGrpcMock(e fixenv.Env) *Session {
	f := func() (*fixenv.GenericResult[*Session], error) {
		s := newTestSession(fmt.Sprintf("test-session-id-%v", e.T().Name()))
		s.client = QueryGrpcMock(e)

		return fixenv.NewGenericResult(s), nil
	}

	return fixenv.CacheResult(e, f)
}

func QueryGrpcMock(e fixenv.Env) *MockQueryServiceClient {
	f := func() (*fixenv.GenericResult[*MockQueryServiceClient], error) {
		m := NewMockQueryServiceClient(MockController(e))

		return fixenv.NewGenericResult(m), nil
	}

	return fixenv.CacheResult(e, f)
}

func MockController(e fixenv.Env) *gomock.Controller {
	f := func() (*fixenv.GenericResult[*gomock.Controller], error) {
		mc := gomock.NewController(e.T().(gomock.TestReporter))

		return fixenv.NewGenericResult(mc), nil
	}

	return fixenv.CacheResult(e, f)
}
