package version

import (
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type version struct {
	Major  string
	Minor  uint64
	Patch  uint64
	Suffix string
}

func (lhs version) Less(rhs version) bool {
	if lhs.Major < rhs.Major {
		return true
	}
	if lhs.Major > rhs.Major {
		return false
	}
	if lhs.Minor < rhs.Minor {
		return true
	}
	if lhs.Minor > rhs.Minor {
		return false
	}
	if lhs.Patch < rhs.Patch {
		return true
	}
	if lhs.Patch > rhs.Patch {
		return false
	}

	return lhs.Suffix < rhs.Suffix
}

// Lt compare lhs and rhs as (lhs < rhs)
func Lt(lhs, rhs string) bool {
	v1, err := Parse(lhs)
	if err != nil {
		return false
	}
	v2, err := Parse(rhs)
	if err != nil {
		return false
	}

	return v1.Less(v2)
}

// Gte compare lhs and rhs as (lhs >= rhs)
func Gte(lhs, rhs string) bool {
	v1, err := Parse(lhs)
	if err != nil {
		return false
	}
	v2, err := Parse(rhs)
	if err != nil {
		return false
	}
	if v1.Less(v2) {
		return false
	}

	return true
}

//nolint:mnd
func Parse(s string) (v version, err error) {
	ss := strings.SplitN(s, "-", 2)
	if len(ss) == 2 {
		v.Suffix = ss[1]
	}
	ss = strings.SplitN(ss[0], ".", 4)
	if len(ss) >= 2 {
		ss = append([]string{ss[0] + "." + ss[1]}, ss[2:]...)
	}
	if len(ss) == 3 {
		v.Patch, err = strconv.ParseUint(ss[2], 10, 64)
		if err != nil {
			return version{}, xerrors.WithStackTrace(err)
		}
	}
	if len(ss) >= 2 {
		v.Minor, err = strconv.ParseUint(ss[1], 10, 64)
		if err != nil {
			return version{}, xerrors.WithStackTrace(err)
		}
	}
	v.Major = ss[0]

	return v, nil
}
