package runner

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const modulePath = "github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts"

var versionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type commandOptions struct {
	version string
	test    string
	list    bool
}

type executeTestFunc func(
	context.Context,
	string,
	string,
	featureTest,
	io.Writer,
	io.Writer,
) error

// Run executes the research CLI and returns its exit code.
func Run(ctx context.Context, in io.Reader, out, errOut io.Writer, args []string) int {
	opts, err := parseFlags(args, errOut)
	if err != nil {
		return 2
	}
	root, err := findModuleRoot()
	if err != nil {
		fmt.Fprintln(errOut, "Error:", err)

		return 1
	}
	tests, err := discoverTests(root)
	if err != nil {
		fmt.Fprintln(errOut, "Error:", err)

		return 1
	}
	if opts.list {
		printTests(out, buildTestMenu(tests))

		return 0
	}

	reader := bufio.NewReader(in)
	if opts.test == "" {
		return runInteractive(ctx, reader, out, errOut, root, tests, opts.version, execute)
	}

	version, err := selectVersion(reader, out, opts.version)
	if err != nil {
		fmt.Fprintln(errOut, "Error:", err)

		return 1
	}
	selected, err := resolveTest(tests, opts.test)
	if err != nil {
		fmt.Fprintln(errOut, "Error:", err)

		return 1
	}

	return executeOnce(ctx, root, version, selected, out, errOut, execute)
}

func executeOnce(
	ctx context.Context,
	root, version string,
	test featureTest,
	out, errOut io.Writer,
	executeTest executeTestFunc,
) int {
	if err := executeTest(ctx, root, version, test, out, errOut); err != nil {
		fmt.Fprintln(errOut, "\nResult: ERROR")
		fmt.Fprintln(errOut, "Error:", err)

		return 1
	}
	fmt.Fprintln(out, "\nResult: RECORDED")

	return 0
}

func runInteractive(
	ctx context.Context,
	reader *bufio.Reader,
	out, errOut io.Writer,
	root string,
	tests []featureTest,
	suppliedVersion string,
	executeTest executeTestFunc,
) int {
	menuRoot := buildTestMenu(tests)
	versionOverride := suppliedVersion
	exitCode := 0
	for {
		version, exit, err := selectInteractiveVersion(reader, out, versionOverride)
		versionOverride = ""
		if errors.Is(err, io.EOF) || exit {
			return exitCode
		}
		if err != nil {
			fmt.Fprintln(errOut, "Error:", err)

			continue
		}

		current := menuRoot
		for {
			selected, back, err := selectTestMenu(reader, out, current)
			if errors.Is(err, io.EOF) {
				return exitCode
			}
			if err != nil {
				fmt.Fprintln(errOut, "Error:", err)

				return 1
			}
			if back {
				if current.parent == nil {
					break
				}
				current = current.parent

				continue
			}
			if selected.test == nil {
				current = selected

				continue
			}

			if executeOnce(ctx, root, version, *selected.test, out, errOut, executeTest) != 0 {
				exitCode = 1
			}
			if ctx.Err() != nil {
				return 1
			}
		}
	}
}

func parseFlags(args []string, errOut io.Writer) (commandOptions, error) {
	var opts commandOptions
	flags := flag.NewFlagSet("server-contracts", flag.ContinueOnError)
	flags.SetOutput(errOut)
	flags.StringVar(&opts.version, "version", "", "YDB Docker image tag; prompt when omitted")
	flags.StringVar(&opts.test, "test", "", "feature file path relative to features/; prompt when omitted")
	flags.BoolVar(&opts.list, "list", false, "list available tests and exit")
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}

	return opts, nil
}

func findModuleRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get current directory: %w", err)
	}
	for {
		contents, readErr := os.ReadFile(filepath.Join(directory, "go.mod"))
		if readErr == nil && strings.Contains(string(contents), "module "+modulePath) {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}

	return "", errors.New("run this command from tests/server_contracts or one of its subdirectories")
}

func selectVersion(reader *bufio.Reader, out io.Writer, supplied string) (string, error) {
	value := supplied
	if value == "" {
		var err error
		value, err = prompt(reader, out, "YDB version [latest] (for example 25.2 or nightly): ")
		if err != nil {
			return "", err
		}
		if value == "" {
			value = "latest"
		}
	}
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "v.")
	if len(value) > 1 && value[0] == 'v' && value[1] >= '0' && value[1] <= '9' {
		value = value[1:]
	}
	if !versionPattern.MatchString(value) {
		return "", fmt.Errorf("invalid YDB Docker tag %q", value)
	}

	return value, nil
}

func selectInteractiveVersion(
	reader *bufio.Reader,
	out io.Writer,
	supplied string,
) (version string, exit bool, err error) {
	if supplied != "" {
		version, err = selectVersion(reader, out, supplied)

		return version, false, err
	}

	fmt.Fprintln(out, "\nYDB version:")
	fmt.Fprintln(out, "  0) Exit")
	value, err := prompt(reader, out, "Docker image tag [latest] (for example 25.2 or nightly): ")
	if err != nil {
		return "", false, err
	}
	if value == "0" {
		return "", true, nil
	}
	if value == "" {
		value = "latest"
	}
	version, err = selectVersion(reader, out, value)

	return version, false, err
}

func prompt(reader *bufio.Reader, out io.Writer, label string) (string, error) {
	fmt.Fprint(out, label)
	value, err := reader.ReadString('\n')
	if errors.Is(err, io.EOF) && value == "" {
		return "", io.EOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read input: %w", err)
	}

	return strings.TrimSpace(value), nil
}
