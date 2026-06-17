package framework

import (
	"os"
	"path/filepath"
	"runtime/pprof"
)

const profileDir = "/profiles"

func startCPUProfile() (*os.File, error) {
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		return nil, err
	}

	file, err := os.Create(filepath.Join(profileDir, "cpu.prof"))
	if err != nil {
		return nil, err
	}

	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()

		return nil, err
	}

	return file, nil
}

func stopCPUProfile(file *os.File) {
	if file == nil {
		return
	}

	pprof.StopCPUProfile()
	_ = file.Close()
}

func writeHeapProfile() error {
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		return err
	}

	file, err := os.Create(filepath.Join(profileDir, "heap.prof"))
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return pprof.WriteHeapProfile(file)
}
