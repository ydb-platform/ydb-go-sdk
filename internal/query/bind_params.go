package query

import (
	"strconv"
	"unicode"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type paramType int

const (
	paramTypePositional = paramType(iota)
	paramTypeNumeric
)

var (
	paramTypesChars = map[paramType]string{
		paramTypePositional: "?",
		paramTypeNumeric:    "$",
	}
	endByStart = map[string]string{
		"/*": "*/",
		"--": "\n",
		"'":  "'",
		"\"": "\"",
		"`":  "`",
	}
	starts = func() (starts []string) {
		for k := range endByStart {
			starts = append(starts, k)
		}
		return starts
	}()
)

func bindParams(query string, t paramType, visitor func(paramName string)) (string, error) {
	var (
		i      = 0
		j      = 0
		ch     = paramTypesChars[t]
		buffer = allocator.Buffers.Get()
		tokens = append(append(make([]string, 0, len(starts)+1), starts...), ch)
	)
	defer allocator.Buffers.Put(buffer)
	for {
		idx, ss := indexAny(query[i:], tokens...)
		switch {
		case idx == -1:
			buffer.WriteString(query[i:])
			return buffer.String(), nil
		case ss == paramTypesChars[paramTypePositional]:
			buffer.WriteString(query[i : i+idx])
			i += idx + len(ch)
			paramName := "$p" + strconv.Itoa(j)
			buffer.WriteString(paramName)
			j++
			if visitor != nil {
				visitor(paramName)
			}
		case ss == paramTypesChars[paramTypeNumeric]:
			buffer.WriteString(query[i : i+idx])
			i += idx + len(ch)
			chars := ""
			for k := 0; k < len(query)-i; k++ {
				ch := query[i+k]
				if !unicode.IsDigit(rune(ch)) {
					break
				}
				chars += string(ch)
			}
			i += len(chars)
			num, err := strconv.Atoi(chars)
			if err != nil {
				return "", xerrors.WithStackTrace(err)
			}
			if num == 0 {
				return "", xerrors.WithStackTrace(ErrUnexpectedNumericArgZero)
			}
			paramName := "$p" + strconv.Itoa(num-1)
			buffer.WriteString(paramName)
			if visitor != nil {
				visitor(paramName)
			}
		default:
			chEnd := endByStart[ss]
			sss := query[i+idx+len(ss):]
			idxEnd := indexNotEscaped(sss, chEnd)
			if idxEnd == -1 {
				idxEnd = len(query)
			} else {
				idxEnd += i + idx + len(ss) + len(chEnd)
			}
			sss = query[i:idxEnd]
			buffer.WriteString(sss)
			i = idxEnd
		}
	}
}
