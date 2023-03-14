package bind

import "regexp"

var (
	// regexps for detect one of type of query
	numericArgsRe    = regexp.MustCompile(`\$\d+`)
	positionalArgsRe = regexp.MustCompile(`[^\\][?]`)
	ydbArgsRe        = regexp.MustCompile(`\$[a-zA-Z\_]+\w*`)
)

type argsType int

const (
	undefinedArgs = argsType(1 << iota >> 1)
	numericArgs
	positionalArgs
	ydbArgs
)

func queryType(q string) (t argsType) {
	if numericArgsRe.MatchString(q) {
		t |= numericArgs
	}
	if positionalArgsRe.MatchString(q) {
		t |= positionalArgs
	}
	if ydbArgsRe.MatchString(q) {
		t |= ydbArgs
	}
	return t
}
