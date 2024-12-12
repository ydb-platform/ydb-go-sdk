//go:build integration && go1.23
// +build integration,go1.23

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestMessageReaderIterator(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("asd")},
		topicwriter.Message{Data: strings.NewReader("ddd")},
		topicwriter.Message{Data: strings.NewReader("ggg")},
	)
	require.NoError(t, err)

	var results []string
	for mess, err := range topicsugar.TopicMessageIterator(ctx, scope.TopicReader()) {
		require.NoError(t, err)
		content, err := io.ReadAll(mess)
		require.NoError(t, err)

		results = append(results, string(content))
		if len(results) == 3 {
			break
		}
	}
	require.Equal(t, []string{"asd", "ddd", "ggg"}, results)
}

func TestStringReaderIterator(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("asd")},
		topicwriter.Message{Data: strings.NewReader("ddd")},
		topicwriter.Message{Data: strings.NewReader("ggg")},
	)
	require.NoError(t, err)

	var results []string
	for mess, err := range topicsugar.StringIterator(ctx, scope.TopicReader()) {
		require.NoError(t, err)

		results = append(results, mess.Data)
		if len(results) == 3 {
			break
		}
	}
	require.Equal(t, []string{"asd", "ddd", "ggg"}, results)
}

func TestMessageJsonUnmarshalIterator(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	marshal := func(d any) io.Reader {
		content, err := json.Marshal(d)
		require.NoError(t, err)
		return bytes.NewReader(content)
	}

	type testStruct struct {
		A int
		B string
	}

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: marshal(testStruct{A: 1, B: "asd"})},
		topicwriter.Message{Data: marshal(testStruct{A: 2, B: "fff"})},
		topicwriter.Message{Data: marshal(testStruct{A: 5, B: "qwe"})},
	)
	require.NoError(t, err)

	var results []testStruct
	expectedSeqno := int64(1)
	expectedOffset := int64(0)
	for mess, err := range topicsugar.JSONIterator[testStruct](ctx, scope.TopicReader()) {
		require.NoError(t, err)
		require.Equal(t, expectedSeqno, mess.SeqNo)
		require.Equal(t, expectedOffset, mess.Offset)

		results = append(results, mess.Data)
		if len(results) == 3 {
			break
		}
		expectedSeqno++
		expectedOffset++
	}

	expectedResult := []testStruct{
		{A: 1, B: "asd"},
		{A: 2, B: "fff"},
		{A: 5, B: "qwe"},
	}
	require.Equal(t, expectedResult, results)
}

func TestCDCReaderIterator(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%s");

ALTER TABLE %s
ADD CHANGEFEED cdc WITH (
	FORMAT='JSON',
	MODE='NEW_AND_OLD_IMAGES'
)
`, scope.Folder(), scope.TableName())

	_, err := scope.Driver().Scripting().Execute(ctx, query, nil)
	require.NoError(t, err)

	query = fmt.Sprintf(`
PRAGMA TablePathPrefix("%s");

ALTER TOPIC %s
ADD CONSUMER %s;
`, scope.Folder(), "`"+scope.TableName()+"/cdc`", "`"+scope.TopicConsumerName()+"`")

	_, err = scope.Driver().Scripting().Execute(ctx, query, nil)
	require.NoError(t, err)

	require.Equal(t, "table", scope.TableName())

	prefix := fmt.Sprintf(`PRAGMA TablePathPrefix("%s");`, scope.Folder())

	err = scope.Driver().Query().Exec(ctx, prefix+`UPSERT INTO table (id, val) VALUES (4124, "asd")`)
	require.NoError(t, err)

	err = scope.Driver().Query().Exec(ctx, prefix+`UPDATE table SET val="qwe"`)
	require.NoError(t, err)

	err = scope.Driver().Query().Exec(ctx, prefix+`DELETE FROM table`)
	require.NoError(t, err)

	cdcPath := path.Join(scope.TablePath(), "cdc")
	reader, err := scope.Driver().Topic().StartReader(scope.TopicConsumerName(), topicoptions.ReadTopic(cdcPath))
	require.NoError(t, err)

	var results []*topicsugar.TypedTopicMessage[topicsugar.YDBCDCMessage[*testCDCItem, int64]]
	for event, err := range topicsugar.UnmarshalCDCStream[*testCDCItem, int64](ctx, reader) {
		require.NoError(t, err)
		results = append(results, event)
		if len(results) == 3 {
			break
		}
	}

	require.Equal(t, &testCDCItem{ID: 4124, Val: "asd"}, results[0].Data.NewImage)
	require.False(t, results[0].Data.IsErase())

	require.Equal(t, &testCDCItem{ID: 4124, Val: "asd"}, results[1].Data.OldImage)
	require.Equal(t, &testCDCItem{ID: 4124, Val: "qwe"}, results[1].Data.NewImage)
	require.False(t, results[0].Data.IsErase())

	require.Equal(t, &testCDCItem{ID: 4124, Val: "qwe"}, results[2].Data.OldImage)
	require.True(t, results[2].Data.IsErase())
}

type testCDCItem struct {
	ID  int64
	Val string
}

func (t *testCDCItem) ParseCDCKey(messages []json.RawMessage) (int64, error) {
	var key int64
	err := json.Unmarshal(messages[0], &key)
	return key, err
}

func (t *testCDCItem) SetPrimaryKey(k int64) {
	t.ID = k
}
