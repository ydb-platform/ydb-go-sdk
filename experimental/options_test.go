package experimental

import (
	"testing"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Experimental"
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
