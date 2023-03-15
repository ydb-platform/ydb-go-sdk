package bind

import (
	"regexp"
	"strings"
)

func clearQuery(q string) string {
	return removeEmptyLines(removeComments(q))
}

var sqlCommentRe = regexp.MustCompile(`((--[^\r\n]*)|(\#[^\r\n]*)|(\/\*[\w\W]*?(\*\/)?\*\/))`)

func removeComments(s string) string {
	return sqlCommentRe.ReplaceAllString(s, "")
}

func removeEmptyLines(s string) string {
	var (
		ss = strings.Split(s, "\n")
		i  = 0
	)
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			ss[i] = s
			i++
		}
	}
	return strings.Join(ss[:i], "\n")
}
