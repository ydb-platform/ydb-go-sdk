package bind

func (b Bindings) pragmas() (pragmas []string) {
	if b.TablePathPrefix != "" {
		pragmas = append(pragmas, "PRAGMA TablePathPrefix(\""+b.TablePathPrefix+"\")")
	}
	return pragmas
}
