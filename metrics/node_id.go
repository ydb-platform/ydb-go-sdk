package metrics

import (
	"strconv"
)

func idToString(id uint32) string {
	return strconv.FormatUint(uint64(id), 10)
}
