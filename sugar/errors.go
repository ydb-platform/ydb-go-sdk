package sugar

import "regexp"

var re = regexp.MustCompile("\\s+at\\s+`[^`]+`")

func removeStackRecords(s string) string {
	return re.ReplaceAllString(s, "")
}

// PrintErrorWithoutStack removed stacktrace records from error string
func PrintErrorWithoutStack(err error) string {
	return removeStackRecords(err.Error())
}
