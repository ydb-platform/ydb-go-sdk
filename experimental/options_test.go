package experimental

import (
	"testing"

	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Experimental"
)

func TestOptionsStreamQueryRequest(t *testing.T) {
	{
		opt := WithStreamQueryProfile(StreamQueryProfileFull)
		req := Ydb_Experimental.ExecuteStreamQueryRequest{}
		opt((*streamQueryDesc)(&req))
		if req.ProfileMode != Ydb_Experimental.ExecuteStreamQueryRequest_FULL {
			t.Errorf("Profile mode is not as expected")
		}
	}
}
