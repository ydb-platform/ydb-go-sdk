package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var (
	goLogPrefix     = regexp.MustCompile(`^\s*[A-Za-z0-9_.-]+\.go:\d+:\s?`)
	goStepReference = regexp.MustCompile(`\s+#\s+[^ ]+\.go:\d+\s+->.*$`)
)

//nolint:tagliatelle // Match the field names defined by go test -json.
type goTestEvent struct {
	Action string `json:"Action"`
	Output string `json:"Output"`
}

func execute(
	ctx context.Context,
	root, version string,
	test featureTest,
	out, errOut io.Writer,
) (finalErr error) {
	if err := checkCompose(ctx); err != nil {
		return err
	}
	stack, err := newComposeStack(ctx, version, out, errOut)
	if err != nil {
		return err
	}
	defer func() {
		finalErr = errors.Join(finalErr, stack.cleanup())
	}()

	fmt.Fprintf(out, "\nStarting YDB %s with Docker Compose...\n", version)
	if err := stack.Up(ctx); err != nil {
		_ = stack.Logs(context.Background())

		return err
	}
	connectionString := fmt.Sprintf("grpc://127.0.0.1:%d/local", stack.grpcPort)
	fmt.Fprintf(out, "Waiting for %s...\n", connectionString)
	if err := waitForYDB(ctx, connectionString); err != nil {
		_ = stack.Logs(context.Background())

		return err
	}
	fmt.Fprintln(out, "YDB is ready.")
	identity := ydbRuntimeIdentity{
		image:    stack.ImageMetadata(ctx),
		imageTag: version,
	}
	identity.serverVersions, identity.serverVersionAPI, identity.serverVersionErr = ydbServerVersions(
		ctx,
		connectionString,
		fmt.Sprintf("http://127.0.0.1:%d", stack.monitoringPort),
	)
	fmt.Fprintln(out)
	printYDBRuntimeIdentity(out, identity)
	fmt.Fprintf(out, "Running: %s (%s)\n\n", test.title, test.path)

	return runGoTest(ctx, root, connectionString, test, out, errOut)
}

func runGoTest(
	ctx context.Context,
	root, connectionString string,
	test featureTest,
	out, errOut io.Writer,
) error {
	command := exec.CommandContext(
		ctx,
		"go",
		"test",
		"-json",
		"-count=1",
		"-run",
		"^TestServerFeatures$",
		test.packagePath,
	)
	command.Dir = root
	command.WaitDelay = 5 * time.Second
	baseEnvironment := removeEnvironment(
		os.Environ(),
		"YDB_ACCESS_TOKEN_CREDENTIALS",
		"YDB_SSL_ROOT_CERTIFICATES_FILE",
	)
	command.Env = replaceEnvironment(baseEnvironment, map[string]string{
		"YDB_CONNECTION_STRING":     connectionString,
		"YDB_SERVER_FEATURE_PATH":   test.featurePath,
		"YDB_RESEARCH_NO_COLORS":    "1",
		"YDB_TOPIC_RESEARCH_FORMAT": "pretty",
	})
	stdout, err := command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture test output: %w", err)
	}
	command.Stderr = errOut
	if err := command.Start(); err != nil {
		return fmt.Errorf("start selected test: %w", err)
	}

	scanErr := forwardTestOutput(stdout, out)
	if scanErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if waitErr != nil {
		waitErr = fmt.Errorf("selected test failed: %w", waitErr)
	}

	return errors.Join(scanErr, waitErr)
}

func forwardTestOutput(input io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var pending string
	for scanner.Scan() {
		var event goTestEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			fmt.Fprintln(out, scanner.Text())

			continue
		}
		if event.Action != "output" {
			continue
		}
		// test2json may split a long output line across multiple events.
		pending += event.Output
		for {
			line, rest, complete := strings.Cut(pending, "\n")
			if !complete {
				break
			}
			pending = rest
			if cleaned, ok := cleanGoTestLine(line); ok {
				fmt.Fprintln(out, cleaned)
			}
		}
	}
	if pending != "" {
		if cleaned, ok := cleanGoTestLine(pending); ok {
			fmt.Fprintln(out, cleaned)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read selected test output: %w", err)
	}

	return nil
}

func replaceEnvironment(environment []string, replacements map[string]string) []string {
	result := make([]string, 0, len(environment)+len(replacements))
	for _, entry := range environment {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, replace := replacements[key]; replace {
				continue
			}
		}
		result = append(result, entry)
	}
	for key, value := range replacements {
		result = append(result, key+"="+value)
	}

	return result
}

func removeEnvironment(environment []string, keys ...string) []string {
	removed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		removed[key] = struct{}{}
	}
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, remove := removed[key]; remove {
				continue
			}
		}
		result = append(result, entry)
	}

	return result
}

func cleanGoTestLine(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	switch {
	case trimmed == "":
		return "", true
	case strings.HasPrefix(trimmed, "=== RUN"):
		return "", false
	case strings.HasPrefix(trimmed, "=== PAUSE"):
		return "", false
	case strings.HasPrefix(trimmed, "=== CONT"):
		return "", false
	case strings.HasPrefix(trimmed, "--- PASS"):
		return "", false
	case strings.HasPrefix(trimmed, "--- FAIL"):
		return "", false
	case strings.HasPrefix(trimmed, "--- SKIP"):
		return "", false
	case trimmed == "PASS" || trimmed == "FAIL":
		return "", false
	case strings.HasPrefix(trimmed, "ok  \t") || strings.HasPrefix(trimmed, "FAIL\t"):
		return "", false
	}

	line = goLogPrefix.ReplaceAllString(line, "")
	line = goStepReference.ReplaceAllString(line, "")

	return strings.TrimRight(line, " \t"), true
}
