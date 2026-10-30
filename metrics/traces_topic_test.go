package metrics

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithTracesInstallsTopicTraceOption(t *testing.T) {
	registry := newRecordingRegistry()
	option := WithTraces(recordingConfig{
		registry: registry,
		details:  trace.DetailsAll,
	})

	driver := &ydb.Driver{}
	require.NoError(t, option(context.Background(), driver))

	// The topic client is created lazily by Driver.connect. Inspecting the
	// option queue here verifies that WithTraces wires the topic tracer into
	// the driver, rather than merely constructing a standalone trace.Topic.
	topicOptions := reflect.ValueOf(driver).Elem().FieldByName("topicOptions")
	require.Equal(t, 1, topicOptions.Len())
}
