package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInteractiveMenuReturnsToPreviousLevelAfterEachRun(t *testing.T) {
	tests := []featureTest{
		{path: "topic/features/research/sample.feature", title: "Research feature"},
		{path: "topic/features/new-category/deep/other.feature", title: "Another feature"},
	}
	input := strings.Join([]string{
		"latest", // version
		"1",      // topic
		"1",      // features
		"2",      // research
		"99",     // invalid choice stays here
		"text",   // invalid choice stays here
		"1",      // run the feature
		"1",      // run again from the same menu
		"0",      // research -> features
		"1",      // new-category
		"1",      // deep
		"1",      // run the other feature
		"0",      // deep -> new-category
		"0",      // new-category -> features
		"0",      // features -> topic
		"0",      // topic -> root
		"0",      // root -> version
		"0",      // exit
		"",
	}, "\n")

	var executed []string
	executeTest := func(
		_ context.Context,
		_, version string,
		test featureTest,
		_, _ io.Writer,
	) error {
		if version != "latest" {
			t.Fatalf("executed with version %q", version)
		}
		executed = append(executed, test.path)

		return nil
	}
	var output bytes.Buffer
	code := runInteractive(
		context.Background(),
		bufio.NewReader(strings.NewReader(input)),
		&output,
		&output,
		"module-root",
		tests,
		"",
		executeTest,
	)
	if code != 0 {
		t.Fatalf("runInteractive returned %d:\n%s", code, output.String())
	}
	wantExecuted := []string{tests[0].path, tests[0].path, tests[1].path}
	if strings.Join(executed, ",") != strings.Join(wantExecuted, ",") {
		t.Fatalf("executed %v, want %v", executed, wantExecuted)
	}
	if count := strings.Count(output.String(), "Tests /topic/features/research:"); count != 5 {
		t.Fatalf("research menu printed %d times, want 5:\n%s", count, output.String())
	}
	for _, expected := range []string{
		"0) Exit", "0) Back", "Research feature (sample.feature)", "Another feature (other.feature)",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("interactive output does not contain %q:\n%s", expected, output.String())
		}
	}
}

func TestExecuteOnceUsesResearchResultVocabulary(t *testing.T) {
	tests := []struct {
		executeErr error
		wantCode   int
		want       string
	}{
		{want: "Result: RECORDED\n"},
		{
			executeErr: errors.New("observer unavailable"),
			wantCode:   1,
			want:       "Result: ERROR\nError: observer unavailable\n",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var output bytes.Buffer
			executeTest := func(
				context.Context,
				string,
				string,
				featureTest,
				io.Writer,
				io.Writer,
			) error {
				return test.executeErr
			}
			code := executeOnce(
				context.Background(),
				"module-root",
				"trunk",
				featureTest{},
				&output,
				&output,
				executeTest,
			)
			if code != test.wantCode {
				t.Fatalf("executeOnce returned %d, want %d", code, test.wantCode)
			}
			if got := strings.TrimPrefix(output.String(), "\n"); got != test.want {
				t.Fatalf("unexpected output %q, want %q", got, test.want)
			}
		})
	}
}

func TestDiscoverTests(t *testing.T) {
	root := t.TempDir()
	writeFeature(t, root, "topic/features/research/first.feature", `@research
Feature: Exact Feature title — имя теста
  Scenario: Scenario title must not become a menu entry
    * something
  Scenario: Another scenario in the same feature
    * something else
`)
	writeFeature(t, root, "table/features/contracts/table.feature", `Feature: Table behavior
  Scenario: Untagged scenario
    * something
`)
	writeFeature(t, root, "topic/features/new-category/nested/second.feature", `@contract
Feature: Exact Feature title — имя теста
  Scenario: Same title in a different file is allowed
    * something
`)
	tests, err := discoverTests(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(tests) != 3 {
		t.Fatalf("got %d entries, want one per feature file", len(tests))
	}
	selected, err := resolveTest(tests, "./topic/features/research/first.feature")
	if err != nil {
		t.Fatal(err)
	}
	if selected.title != "Exact Feature title — имя теста" ||
		selected.featurePath != "features/research/first.feature" ||
		selected.packagePath != "./topic" {
		t.Fatalf("unexpected selected feature: %+v", selected)
	}
	var output bytes.Buffer
	printTests(&output, buildTestMenu(tests))
	want := `Available tests (relative to this module):
  table/
    features/
      contracts/
        Table behavior (table.feature)
  topic/
    features/
      new-category/
        nested/
          Exact Feature title — имя теста (second.feature)
      research/
        Exact Feature title — имя теста (first.feature)
`
	if output.String() != want {
		t.Fatalf("unexpected feature tree:\n%s\nwant:\n%s", output.String(), want)
	}
	for _, unknown := range []string{
		"1", "topic/scenario-title", "missing.feature", "../topic/features/research/first.feature",
	} {
		if _, err := resolveTest(tests, unknown); err == nil {
			t.Errorf("accepted unknown feature %q", unknown)
		}
	}

	writeFeature(t, root, "topic/features/added-later/deeper/third.feature", `Feature: Newly added
  Scenario: New scenario
    * something
`)
	reloaded, err := discoverTests(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded) != 4 {
		t.Fatalf("reload found %d files, want 4", len(reloaded))
	}
	output.Reset()
	printTests(&output, buildTestMenu(reloaded))
	if !strings.Contains(output.String(), "added-later/\n        deeper/\n          Newly added (third.feature)") {
		t.Fatalf("new folder is absent from reloaded menu:\n%s", output.String())
	}
}

func TestDiscoverTestsRejectsInvalidFeatures(t *testing.T) {
	for _, contents := range []string{
		"",
		"Feature:\n  Scenario: Something\n    * action\n",
		"Feature: No scenarios\n",
		"Feature: Broken syntax\n  Scenario: Invalid\n    * action\n  Feature: Second\n",
	} {
		t.Run(contents, func(t *testing.T) {
			root := t.TempDir()
			writeFeature(t, root, "topic/features/research/broken.feature", contents)
			if _, err := discoverTests(root); err == nil || !strings.Contains(err.Error(), "broken.feature") {
				t.Fatalf("expected error identifying the feature file, got %v", err)
			}
		})
	}
	if _, err := discoverTests(t.TempDir()); err == nil {
		t.Fatal("expected error for an empty catalog")
	}
}

func TestSelectVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "", want: "latest"},
		{input: "v25.2", want: "25.2"},
		{input: "v.25.2.1.24", want: "25.2.1.24"},
		{input: "nightly", want: "nightly"},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(test.input + "\n"))
			got, err := selectVersion(reader, &bytes.Buffer{}, "")
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestCleanGoTestLine(t *testing.T) {
	tests := []struct {
		input string
		want  string
		keep  bool
	}{
		{input: "=== RUN   TestServerFeatures", keep: false},
		{input: "--- PASS: TestServerFeatures (0.1s)", keep: false},
		{
			input: "    testingt.go:121: Research report:",
			want:  "Research report:",
			keep:  true,
		},
		{
			input: "    testingt.go:121:   * TopicService.StreamWrite: InitRequest{}",
			want:  "  * TopicService.StreamWrite: InitRequest{}",
			keep:  true,
		},
		{
			input: "    Given an empty topic # world_test.go:98 -> example.step",
			want:  "    Given an empty topic",
			keep:  true,
		},
	}
	for _, test := range tests {
		got, keep := cleanGoTestLine(test.input)
		if got != test.want || keep != test.keep {
			t.Fatalf("cleanGoTestLine(%q) = (%q, %t), want (%q, %t)", test.input, got, keep, test.want, test.keep)
		}
	}
}

func TestForwardTestOutputPreservesFragmentedLines(t *testing.T) {
	var input bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, output := range []string{
		"=== RUN   TestServerFeatures\n",
		"    testingt.go:1: gRPC message: " + strings.Repeat("x", 2048),
		" continued\n    testingt.go:2: next response\n",
		"PASS\nlast line without newline",
	} {
		if err := encoder.Encode(goTestEvent{Action: "output", Output: output}); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := forwardTestOutput(&input, &output); err != nil {
		t.Fatal(err)
	}
	want := "gRPC message: " + strings.Repeat("x", 2048) +
		" continued\nnext response\nlast line without newline\n"
	if output.String() != want {
		t.Fatalf("fragmented trace was changed: %q", output.String())
	}
}

func TestComposeYAML(t *testing.T) {
	compose := composeYAML("25.2", 32136, 38765)
	for _, expected := range []string{
		"image: ydbplatform/local-ydb:25.2",
		`GRPC_PORT: "32136"`,
		`"127.0.0.1:32136:32136"`,
		`"127.0.0.1:38765:8765"`,
		`YDB_USE_IN_MEMORY_PDISKS: "true"`,
	} {
		if !strings.Contains(compose, expected) {
			t.Fatalf("Compose file does not contain %q:\n%s", expected, compose)
		}
	}
}

func TestPrintYDBRuntimeIdentity(t *testing.T) {
	identity := ydbRuntimeIdentity{
		serverVersions:   []string{"trunk-2026-08-28"},
		serverVersionAPI: "Viewer API",
		imageTag:         "trunk",
		image: dockerImageMetadata{
			versionLabel: "nightly",
			revision:     "1b5929795302af7040599fec22c55c59dcaaf792",
			digest:       "sha256:c3e49c078a560d4957ff492ed7025864ee33a405d03a02c4fdb30c20e7c42925",
		},
	}

	var output bytes.Buffer
	printYDBRuntimeIdentity(&output, identity)
	want := `YDB server version (Viewer API): trunk-2026-08-28
Docker image: ydbplatform/local-ydb:trunk@sha256:c3e49c078a560d4957ff492ed7025864ee33a405d03a02c4fdb30c20e7c42925
YDB source revision (image): 1b5929795302af7040599fec22c55c59dcaaf792
OCI version label: nightly
`
	if output.String() != want {
		t.Fatalf("unexpected identity output:\n%s\nwant:\n%s", output.String(), want)
	}
}

func TestPrintYDBRuntimeIdentityWithUnavailableAPIAndImageID(t *testing.T) {
	identity := ydbRuntimeIdentity{
		serverVersionAPI: "YDB API",
		serverVersionErr: errors.New("API is not supported"),
		imageTag:         "25.1",
		image: dockerImageMetadata{
			imageID: "sha256:image-id",
		},
	}

	var output bytes.Buffer
	printYDBRuntimeIdentity(&output, identity)
	want := `YDB server version (YDB API): unavailable (API is not supported)
Docker image: ydbplatform/local-ydb:25.1
Docker image ID: sha256:image-id
`
	if output.String() != want {
		t.Fatalf("unexpected identity output:\n%s\nwant:\n%s", output.String(), want)
	}
}

func TestCollectJSONVersions(t *testing.T) {
	document := map[string]any{
		"SystemStateInfo": []any{
			map[string]any{"NodeId": float64(1), "Version": "26.1.1.22"},
			map[string]any{"NodeId": float64(2), "Version": "26.1.1.22"},
			map[string]any{"NodeId": float64(3), "Version": "trunk-abcdef"},
		},
	}
	versions := make(map[string]struct{})
	collectJSONVersions(document, versions)
	if _, ok := versions["26.1.1.22"]; !ok {
		t.Fatal("released version was not collected")
	}
	if _, ok := versions["trunk-abcdef"]; !ok {
		t.Fatal("trunk version was not collected")
	}
	if len(versions) != 2 {
		t.Fatalf("got %d unique versions, want 2", len(versions))
	}
}

func writeFeature(t *testing.T, root, relativePath, contents string) {
	t.Helper()
	filePath := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
