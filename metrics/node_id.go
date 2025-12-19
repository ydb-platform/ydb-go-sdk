package metrics

import (
	"strconv"
)

const (
	trueStr  = "true"
	falseStr = "false"
)

func idToString(id uint32) string {
	return strconv.FormatUint(uint64(id), 10)
}

func nodeHintHitString(preferred, actual string) string {
	if preferred == actual {
		return trueStr
	}
	return falseStr
}
