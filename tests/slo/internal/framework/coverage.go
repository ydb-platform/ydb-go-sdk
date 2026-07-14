package framework

import "os"

func ensureCoverageDir() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}

	_ = os.MkdirAll(dir, 0o755)
}
