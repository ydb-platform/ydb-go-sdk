package witharrow

import (
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var Decode = query.NewArrowDecoder(ipc.NewReader)
