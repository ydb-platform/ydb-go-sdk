package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

var (
	source      = flag.String("source", "internal/proto", "space separated list of paths to proto or grpc specs")
	sourceBase  = flag.String("source-base", "", "source path prefix to be stripped for package name mapping")
	imports     = flag.String("import", "", "space separated list of path for proto import lookup")
	strip       = flag.String("strip", "kikimr/public", "path to strip off from the output")
	compiler    = flag.String("compiler", "protoc", "path to proto files compiler binary")
	compilerCwd = flag.String("compiler-cwd", "", "work dir for the compiler process")
	custom      = flag.String("custom", "bin/protoc-gen", "path to a custom protoc plugin")
	plugins     = flag.String("plugins", "ydb+grpc", "'+' separated list of plugin plugins")
	prefix      = flag.String("repository", "github.com/yandex-cloud/ydb-go-sdk/v2/internal", "go package name prefix")
	dest        = flag.String("destination", "internal", "path to output the generated files")
	verbose     = flag.Bool("verbose", false, "true to log everything")
	dry         = flag.Bool("dry-run", false, "true to debug")
)

var (
	packagePrefix = []byte("package")
)

func eachPath(paths string, f func(string)) {
	for _, path := range strings.Split(paths, " ") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		f(path)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	var (
		m          mapper
		protocArgs []string
	)
	eachPath(*source, func(path string) {
		if err := m.ReadDir(path); err != nil {
			log.Fatal(err)
		}
		protocArgs = append(protocArgs,
			"--proto_path", path,
		)
	})
	eachPath(*imports, func(path string) {
		protocArgs = append(protocArgs,
			"--proto_path", path,
		)
	})
	{
		protocArgs = append(protocArgs,
			"--plugin", "protoc-gen-custom="+*custom,
		)
		n := len(protocArgs)
		protocArgs = protocArgs[0:n:n] // Ensure that append will always copy.
	}
	var (
		alias bytes.Buffer
		n     int

		ready  = make(chan struct{})
		errors = make(chan error)
	)
	m.Walk(func(pkg, path string, files []string) {
		importPath := strings.TrimPrefix(path, *sourceBase)
		importPath = strings.Trim(importPath, "/")

		pkgName := strings.Replace(pkg, ".", "_", -1)
		pkgPath := strings.TrimPrefix(path, *strip)
		goPackage := filepath.Join(*prefix, pkgPath, pkgName)

		if pkgPath != path {
			for _, file := range files {
				if alias.Len() != 0 {
					alias.WriteByte(',')
				}
				alias.WriteByte('M')
				alias.WriteString(filepath.Join(importPath, file))
				alias.WriteByte('=')
				alias.WriteString(goPackage)
			}
		}

		n++
		go func() {
			destPath := filepath.Join(*dest, pkgPath, pkgName)
			err := os.MkdirAll(destPath, 0755)
			if err != nil {
				errors <- err
				return
			}

			// Wait until all packages are mapped.
			<-ready

			// Prepare compilation command.
			args := append(protocArgs,
				"--custom_out", fmt.Sprintf(
					"plugins=%s,%s:%s", *plugins, alias.Bytes(), destPath,
				),
			)
			args = append(args, files...)

			if *verbose || *dry {
				log.Printf("%s %s", *compiler, args)
			}
			if *dry {
				errors <- nil
				return
			}

			cmd := exec.Command(*compiler, args...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				err = fmt.Errorf(
					"error compiling %s: %v (out=%s; args=%v)",
					strings.Join(files, ","), err,
					out, args,
				)
			}
			errors <- err
		}()
	})
	close(ready)
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		err := <-errors
		if err != nil {
			_, _ = fmt.Fprintf(&buf, "error: %v\n", err)
		}
	}
	if buf.Len() > 0 {
		_, _ = io.Copy(os.Stderr, &buf)
		os.Exit(1)
	}
}

type mapper struct {
	once   sync.Once
	err    error
	buf    *bufio.Reader
	files  map[string][]string // Mapping of package name to its files.
	dir    map[string]string   // Mapping of package name to its dir.
	ignore map[string]bool     // Mapping of package name and ignore lag.
}

func (m *mapper) init() {
	m.once.Do(func() {
		m.buf = bufio.NewReader(nil)
		m.files = make(map[string][]string)
		m.dir = make(map[string]string)
		m.ignore = make(map[string]bool)
	})
}

func (m *mapper) ReadDir(root string) (err error) {
	m.init()
	if m.err != nil {
		return m.err
	}
	defer func() {
		if err != nil && m.err == nil {
			m.err = err
		}
	}()

	file, err := os.Open(root)
	if err != nil {
		return
	}
	files, err := file.Readdir(0)
	if err != nil {
		return err
	}
	for _, info := range files {
		if info.IsDir() {
			continue
		}

		name := info.Name()
		if !strings.HasSuffix(name, ".proto") {
			continue
		}
		path := filepath.Join(root, name)

		pkg, err := m.packageName(path)
		if err != nil {
			return err
		}
		if m.ignore[pkg] {
			continue
		}

		m.files[pkg] = append(m.files[pkg], name)

		// Check that all package files belongs to a single directory.
		if dir, has := m.dir[pkg]; !has {
			m.dir[pkg] = root
		} else if dir != root {
			log.Printf(
				"warning: ignoring pacakge %q because it belongs to "+
					"at least these two directories:\n  %v\n  %v",
				pkg, dir, root,
			)
			m.ignore[pkg] = true
			delete(m.files, pkg)
		}
	}
	return m.err
}

func (m *mapper) Walk(it func(pkg, path string, files []string)) {
	if m.err != nil {
		return
	}
	for pkg, files := range m.files {
		it(pkg, m.dir[pkg], files)
	}
}

func (m *mapper) Err() error {
	return m.err
}

func (m *mapper) packageName(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	m.buf.Reset(f)
	for {
		line, err := m.buf.ReadBytes('\n')
		if err != nil {
			return "", err
		}
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, packagePrefix) {
			continue
		}
		i := bytes.IndexByte(line, ';')
		p := bytes.TrimSpace(line[len(packagePrefix):i])
		return string(p), nil
	}
}
