package metrics

import (
	"strconv"
)

func idToString(id int64) string {
	return strconv.FormatInt(id, 10)
}
