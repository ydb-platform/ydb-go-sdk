package bind

func pragmas(tablePathPrefix string) (pragmas []string) {
	if tablePathPrefix != "" {
		pragmas = append(pragmas, "PRAGMA TablePathPrefix(\""+tablePathPrefix+"\")")
	}
	return pragmas
}
