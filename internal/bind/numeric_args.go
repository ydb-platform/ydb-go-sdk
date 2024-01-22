package bind

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type NumericArgs struct{}

func (m NumericArgs) blockID() blockID {
	return blockYQL
}

func (m NumericArgs) RewriteQuery(sql string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	l := &sqlLexer{
		src:        sql,
		stateFn:    numericArgsStateFn,
		rawStateFn: numericArgsStateFn,
	}

	for l.stateFn != nil {
		l.stateFn = l.stateFn(l)
	}

	var (
		buffer = xstring.Buffer()
		param  table.ParameterOption
	)
	defer buffer.Free()

	if len(args) > 0 {
		newArgs = make([]interface{}, len(args))
	}

	for _, p := range l.parts {
		switch p := p.(type) {
		case string:
			buffer.WriteString(p)
		case numericArg:
			if p == 0 {
				return "", nil, xerrors.WithStackTrace(ErrUnexpectedNumericArgZero)
			}
			if int(p) > len(args) {
				return "", nil, xerrors.WithStackTrace(
					fmt.Errorf("%w: $p%d, len(args) = %d", ErrInconsistentArgs, p, len(args)),
				)
			}
			paramName := "$p" + strconv.Itoa(int(p-1)) //nolint:goconst
			if newArgs[p-1] == nil {
				param, err = toYdbParam(paramName, args[p-1])
				if err != nil {
					return "", nil, xerrors.WithStackTrace(err)
				}
				newArgs[p-1] = param
				buffer.WriteString(param.Name())
			} else {
				buffer.WriteString(newArgs[p-1].(table.ParameterOption).Name())
			}
		}
	}

	for i, p := range newArgs {
		if p == nil {
			return "", nil, xerrors.WithStackTrace(
				fmt.Errorf("%w: $p%d, len(args) = %d", ErrInconsistentArgs, i+1, len(args)),
			)
		}
	}

	if len(newArgs) > 0 {
		const prefix = "-- origin query with numeric args replacement\n"
		return prefix + buffer.String(), newArgs, nil
	}

	return buffer.String(), newArgs, nil
}

func numericArgsStateFn(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '`':
			return backtickState
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '$':
			nextRune, _ := utf8.DecodeRuneInString(l.src[l.pos:])
			if isNumber(nextRune) {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos-width])
				}
				l.start = l.pos
				return numericArgState
			}
		case '-':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '-' {
				l.pos += width
				return oneLineCommentState
			}
		case '/':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '*' {
				l.pos += width
				return multilineCommentState
			}
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}
			return nil
		}
	}
}

func numericArgState(l *sqlLexer) stateFn {
	numbers := ""
	defer func() {
		if len(numbers) > 0 {
			i, err := strconv.Atoi(numbers)
			if err != nil {
				panic(err)
			}
			l.parts = append(l.parts, numericArg(i))
			l.start = l.pos
		} else {
			l.parts = append(l.parts, l.src[l.start-1:l.pos])
			l.start = l.pos
		}
	}()
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch {
		case isNumber(r):
			numbers += string(r)
		case isLetter(r):
			numbers = ""
			return l.rawStateFn
		default:
			l.pos -= width
			return l.rawStateFn
		}
	}
}
