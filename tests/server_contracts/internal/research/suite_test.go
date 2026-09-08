package research_test

import (
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/cucumber/godog"
	messages "github.com/cucumber/messages/go/v34"
)

func TestServerFeatures(t *testing.T) {
	options := featureSuiteOptions()
	options.TestingT = t
	suite := godog.TestSuite{
		Name:                "server-features",
		ScenarioInitializer: initializeScenario(),
		Options:             options,
	}

	if status := suite.Run(); status != 0 {
		t.Fatalf("server feature suite failed with status %d", status)
	}
}

func TestFormatResearchScenario(t *testing.T) {
	scenario := &godog.Scenario{
		Name: "Readable research",
		Steps: []*messages.PickleStep{
			{Text: "TopicService.StreamWrite: InitRequest{partition_id: 0}"},
			{
				Text: "TopicService.StreamWrite: WriteRequest messages:",
				Argument: &messages.PickleStepArgument{DataTable: &messages.PickleTable{
					Rows: []*messages.PickleTableRow{
						{Cells: []*messages.PickleTableCell{{Value: "data"}, {Value: "seq_no"}}},
						{Cells: []*messages.PickleTableCell{{Value: "message"}, {Value: "1"}}},
					},
				}},
			},
		},
	}

	want := strings.Join([]string{
		"Scenario: Readable research",
		"  * TopicService.StreamWrite: InitRequest{partition_id: 0}",
		"  * TopicService.StreamWrite: WriteRequest messages:",
		"    | data    | seq_no |",
		"    | message | 1      |",
	}, "\n")
	if got := formatResearchScenario(scenario); got != want {
		t.Fatalf("formatted research scenario:\n%s\nwant:\n%s", got, want)
	}
}

func TestEveryResearchScenarioDocumentsCurrentObservation(t *testing.T) {
	featureFiles := os.DirFS("../../features")
	scenarios := 0
	err := fs.WalkDir(featureFiles, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".feature") {
			return nil
		}
		contents, err := fs.ReadFile(featureFiles, path)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(contents), "\n")
		for i, line := range lines {
			if !strings.HasPrefix(strings.TrimSpace(line), "Scenario:") {
				continue
			}
			scenarios++
			hasObservation := false
			for j := i - 1; j >= 0; j-- {
				previous := strings.TrimSpace(lines[j])
				if !strings.HasPrefix(previous, "#") {
					break
				}
				if strings.HasPrefix(previous, "# Observed on YDB ") {
					hasObservation = true
				}
			}
			if !hasObservation {
				t.Errorf("%s:%d: scenario has no adjacent '# Observed on YDB ...' comment", path, i+1)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scenarios == 0 {
		t.Fatal("no research scenarios checked")
	}
}

func featureSuiteOptions() *godog.Options {
	format := os.Getenv("YDB_RESEARCH_FORMAT")
	if format == "" {
		format = "pretty"
	}
	paths := []string{"."}
	if featurePath := os.Getenv("YDB_SERVER_FEATURE_PATH"); featurePath != "" {
		paths = []string{featurePath}
	}
	options := &godog.Options{
		Format:   format,
		FS:       os.DirFS("../../features"),
		Paths:    paths,
		Strict:   true,
		NoColors: os.Getenv("YDB_RESEARCH_NO_COLORS") != "",
	}
	if format == "pretty" {
		options.Output = io.Discard
	}

	return options
}
