package research_test

import (
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/cucumber/godog"
)

func TestStreamWriteAliasVocabulary(t *testing.T) {
	for _, suffix := range []string{
		`InitRequest\{([^}]*)\}$`,
		`WriteRequest messages:$`,
		`WriteRequest\{([^}]*)\} messages:$`,
		`CloseSend$`,
		`CloseSend before consuming the recorded WriteResponse$`,
	} {
		pattern := regexp.MustCompile(streamWriteStepPrefix + suffix)
		for _, alias := range []string{"", "A"} {
			prefix := "TopicService.StreamWrite"
			if alias != "" {
				prefix += " \"" + alias + "\""
			}
			action := strings.TrimSuffix(suffix, "$")
			if strings.HasPrefix(action, "InitRequest") {
				action = "InitRequest{producer_id: same, partition_id: 0}"
			}
			if strings.HasPrefix(action, `WriteRequest\{`) {
				action = "WriteRequest{txId: Transaction A} messages:"
			}
			matches := pattern.FindStringSubmatch(prefix + ": " + action)
			if len(matches) < 2 || matches[1] != alias {
				t.Fatalf("step %q did not capture alias %q", prefix+": "+action, alias)
			}
		}
	}
}

func TestEveryFeatureUsesRequestScopedTransactions(t *testing.T) {
	t.Setenv("YDB_SERVER_FEATURE_PATH", "")
	features, err := (godog.TestSuite{Options: featureSuiteOptions()}).RetrieveFeatures()
	if err != nil {
		t.Fatal(err)
	}
	writePattern := regexp.MustCompile(streamWriteRequestStepPattern)
	beginPattern := regexp.MustCompile(`^QueryService\.BeginTransaction: .*"([^"]+)"$`)
	requests, batches := 0, 0
	for _, feature := range features {
		for _, scenario := range feature.Pickles {
			transactions := make(map[string]*leasedQueryTransaction)
			for _, step := range scenario.Steps {
				if match := beginPattern.FindStringSubmatch(step.Text); match != nil {
					transactions[match[1]] = &leasedQueryTransaction{
						transaction: queryTransactionIDStub{id: match[1]}, sessionID: "session-" + match[1],
					}
				}
				if !strings.HasPrefix(step.Text, "TopicService.StreamWrite") || !strings.Contains(step.Text, ": WriteRequest") {
					continue
				}
				match := writePattern.FindStringSubmatch(step.Text)
				if match == nil || step.Argument == nil || step.Argument.DataTable == nil {
					t.Fatalf("%s: invalid WriteRequest step %q", feature.Uri, step.Text)
				}
				request, err := buildWriteRequest(match[2], step.Argument.DataTable, transactions)
				if err != nil {
					t.Fatalf("%s / %s: %v", feature.Uri, scenario.Name, err)
				}
				requests++
				if len(request.GetWriteRequest().GetMessages()) > 1 {
					batches++
				}
			}
		}
	}
	if requests == 0 || batches == 0 {
		t.Fatalf("checked %d requests with %d multi-message batches", requests, batches)
	}
}

func TestFeatureSuiteSelection(t *testing.T) {
	const selectedPath = "topic/research/nested/selected.feature"
	files := fstest.MapFS{
		selectedPath: {Data: []byte(`Feature: Selected
  Scenario: First
    * action
  Scenario: Second
    * action
`)},
		"query/research/other.feature": {Data: []byte("Feature: Other\n  Scenario: Not selected\n    * action\n")},
	}
	for _, selected := range []string{"", selectedPath} {
		t.Run(selected, func(t *testing.T) {
			t.Setenv("YDB_SERVER_FEATURE_PATH", selected)
			options := featureSuiteOptions()
			options.FS = files
			features, err := (godog.TestSuite{Options: options}).RetrieveFeatures()
			if err != nil {
				t.Fatal(err)
			}
			wantFeatures, wantScenarios := 1, 2
			if selected == "" {
				wantFeatures, wantScenarios = 2, 3
			}
			scenarios := 0
			for _, feature := range features {
				scenarios += len(feature.Pickles)
				if selected != "" && feature.Uri != selected {
					t.Fatalf("selected wrong feature %q", feature.Uri)
				}
			}
			if len(features) != wantFeatures || scenarios != wantScenarios {
				t.Fatalf("selected %d features / %d scenarios, want %d / %d",
					len(features), scenarios, wantFeatures, wantScenarios)
			}
		})
	}
}
