package traces

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type endpoints []trace.EndpointInfo

func (es endpoints) String() string {
	bldr := strings.Builder{}
	bldr.WriteByte('[')
	for i, e := range es {
		bldr.WriteString(e.String())
		if i+1 < len(es) {
			bldr.WriteByte(',')
		}
	}
	bldr.WriteByte(']')
	return bldr.String()
}

type metadata map[string][]string

func (m metadata) String() string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("error:%s", err)
	}
	return string(b)
}
