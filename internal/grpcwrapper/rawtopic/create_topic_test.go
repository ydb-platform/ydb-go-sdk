package rawtopic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

func TestCreateTopicRequestToProto(t *testing.T) {
	t.Run("PassesOperationParams", func(t *testing.T) {
		req := &CreateTopicRequest{
			OperationParams: rawydb.OperationParams{
				OperationMode:    rawydb.OperationParamsModeSync,
				OperationTimeout: rawoptional.Duration{Value: time.Second, HasValue: true},
				CancelAfter:      rawoptional.Duration{Value: 2 * time.Second, HasValue: true},
			},
			Path: "/local/topic",
		}

		proto := req.ToProto()
		require.Equal(t, "/local/topic", proto.GetPath())
		require.Equal(t,
			Ydb_Operations.OperationParams_SYNC,
			proto.GetOperationParams().GetOperationMode(),
		)
		require.Equal(t, time.Second, proto.GetOperationParams().GetOperationTimeout().AsDuration())
		require.Equal(t, 2*time.Second, proto.GetOperationParams().GetCancelAfter().AsDuration())
	})
}
