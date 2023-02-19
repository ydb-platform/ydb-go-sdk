package traces

import (
	"encoding/json"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type endpoints []trace.EndpointInfo

func (es endpoints) String() string {

	b, _ := json.Marshal(es)
	return string(b)

}

type metadata map[string][]string

func (m metadata) String() string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("error:%s", err)
	}
	return string(b)
}
