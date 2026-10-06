package runner

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
)

type featureTest struct {
	title string
	path  string
}

type testMenu struct {
	path     string
	test     *featureTest
	parent   *testMenu
	children []*testMenu
}

func discoverTests(root string) ([]featureTest, error) {
	root = filepath.Join(root, "features")
	var tests []featureTest
	err := filepath.WalkDir(root, func(directory string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && directory != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
			return filepath.SkipDir
		}
		if entry.IsDir() || filepath.Ext(directory) != ".feature" {
			return nil
		}
		relativePath, err := filepath.Rel(root, directory)
		if err != nil {
			return err
		}
		relativePath = filepath.ToSlash(relativePath)
		test, err := parseFeatureTest(root, relativePath)
		if err != nil {
			return err
		}
		tests = append(tests, test)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover feature files: %w", err)
	}
	if len(tests) == 0 {
		return nil, errors.New("no feature files found under features")
	}
	sort.Slice(tests, func(i, j int) bool { return tests[i].path < tests[j].path })

	return tests, nil
}

func parseFeatureTest(root, relativePath string) (featureTest, error) {
	suite := godog.TestSuite{Options: &godog.Options{
		FS:    os.DirFS(root),
		Paths: []string{relativePath},
	}}
	features, err := suite.RetrieveFeatures()
	if err != nil {
		return featureTest{}, err
	}
	if len(features) == 0 {
		return featureTest{}, fmt.Errorf("%s: feature contains no executable scenarios", relativePath)
	}
	feature := features[0]
	if feature.Feature == nil || strings.TrimSpace(feature.Feature.Name) == "" {
		return featureTest{}, fmt.Errorf("%s: missing Feature title", relativePath)
	}

	return featureTest{
		title: feature.Feature.Name,
		path:  relativePath,
	}, nil
}

func buildTestMenu(tests []featureTest) *testMenu {
	root := &testMenu{}
	directories := map[string]*testMenu{"": root}
	for i := range tests {
		parent := root
		parts := strings.Split(tests[i].path, "/")
		for j := range parts[:len(parts)-1] {
			directory := strings.Join(parts[:j+1], "/")
			node, ok := directories[directory]
			if !ok {
				node = &testMenu{path: directory, parent: parent}
				directories[directory] = node
				parent.children = append(parent.children, node)
			}
			parent = node
		}
		parent.children = append(parent.children, &testMenu{
			path: tests[i].path, test: &tests[i], parent: parent,
		})
	}
	for _, directory := range directories {
		sort.Slice(directory.children, func(i, j int) bool {
			left, right := directory.children[i], directory.children[j]
			if (left.test == nil) != (right.test == nil) {
				return left.test == nil
			}

			return left.label() < right.label()
		})
	}

	return root
}

func (menu *testMenu) label() string {
	if menu.test == nil {
		return path.Base(menu.path) + "/"
	}

	return fmt.Sprintf("%s (%s)", menu.test.title, path.Base(menu.path))
}

func selectTestMenu(reader *bufio.Reader, out io.Writer, menu *testMenu) (*testMenu, bool, error) {
	for {
		fmt.Fprintf(out, "\nTests /%s:\n", menu.path)
		for i, child := range menu.children {
			fmt.Fprintf(out, "  %d) %s\n", i+1, child.label())
		}
		fmt.Fprintln(out, "  0) Back")
		value, err := prompt(reader, out, "Choice [1]: ")
		if err != nil {
			return nil, false, err
		}
		if value == "0" {
			return nil, true, nil
		}
		if value == "" {
			value = "1"
		}
		number, err := strconv.Atoi(value)
		if err == nil && number >= 1 && number <= len(menu.children) {
			return menu.children[number-1], false, nil
		}
		fmt.Fprintf(out, "Choose a number between 1 and %d, or 0 to go back.\n", len(menu.children))
	}
}

func resolveTest(tests []featureTest, supplied string) (featureTest, error) {
	filePath := filepath.ToSlash(filepath.Clean(supplied))
	for _, test := range tests {
		if test.path == filePath {
			return test, nil
		}
	}

	return featureTest{}, fmt.Errorf("unknown feature path %q; use -list to see available files", supplied)
}

func printTests(out io.Writer, menu *testMenu) {
	fmt.Fprintln(out, "Available tests (relative to features/):")
	var printChildren func(*testMenu, string)
	printChildren = func(parent *testMenu, indent string) {
		for _, child := range parent.children {
			fmt.Fprintln(out, indent+child.label())
			printChildren(child, indent+"  ")
		}
	}
	printChildren(menu, "  ")
}
