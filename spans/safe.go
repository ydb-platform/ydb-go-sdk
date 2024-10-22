package spans

import (
	"fmt"
	"strconv"
)

func safeAddress(a interface{ Address() string }) string {
	if a == nil {
		return ""
	}

	return a.Address()
}

func safeNodeID(n interface{ NodeID() uint32 }) string {
	if n == nil {
		return "0"
	}

	return strconv.FormatUint(uint64(n.NodeID()), 10)
}

func safeID(id interface{ ID() string }) string {
	if id == nil {
		return ""
	}

	return id.ID()
}

func safeStatus(s interface{ Status() string }) string {
	if s == nil {
		return ""
	}

	return s.Status()
}

func safeStringer(s fmt.Stringer) string {
	if s == nil {
		return ""
	}

	return s.String()
}

func safeError(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func safeErr(err interface{ Err() error }) error {
	if err == nil {
		return nil
	}

	return err.Err()
}
