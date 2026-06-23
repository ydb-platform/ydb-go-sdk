//go:build go1.23

package topicsugar

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// messagesReader is a TopicMessageReader stub that yields the prepared messages
// in order and then returns io.EOF.
type messagesReader struct {
	messages []*topicreader.Message
	index    int
}

func (r *messagesReader) ReadMessage(context.Context) (*topicreader.Message, error) {
	if r.index >= len(r.messages) {
		return nil, io.EOF
	}
	msg := r.messages[r.index]
	r.index++

	return msg, nil
}

func messageWithData(t *testing.T, data []byte) *topicreader.Message {
	t.Helper()

	return topicreadercommon.NewPublicMessageBuilder().DataAndUncompressedSize(data).Build()
}

func TestProtobufIterator(t *testing.T) {
	expected := timestamppb.New(time.Unix(1718000000, 0).UTC())
	data, err := proto.Marshal(expected)
	require.NoError(t, err)

	reader := &messagesReader{
		messages: []*topicreader.Message{messageWithData(t, data)},
	}

	var got *timestamppb.Timestamp
	for msg, err := range ProtobufIterator[*timestamppb.Timestamp](context.Background(), reader) {
		if err != nil {
			require.ErrorIs(t, err, io.EOF)

			break
		}
		got = msg.Data
	}

	require.NotNil(t, got)
	require.True(t, proto.Equal(expected, got))
}

func TestProtobufIteratorMultipleMessages(t *testing.T) {
	expected := []*timestamppb.Timestamp{
		timestamppb.New(time.Unix(1718000000, 0).UTC()),
		timestamppb.New(time.Unix(1719000000, 0).UTC()),
		timestamppb.New(time.Unix(1720000000, 0).UTC()),
	}

	reader := &messagesReader{}
	for _, ts := range expected {
		data, err := proto.Marshal(ts)
		require.NoError(t, err)
		reader.messages = append(reader.messages, messageWithData(t, data))
	}

	var got []*timestamppb.Timestamp
	for msg, err := range ProtobufIterator[*timestamppb.Timestamp](context.Background(), reader) {
		if err != nil {
			require.ErrorIs(t, err, io.EOF)

			break
		}
		got = append(got, msg.Data)
	}

	require.Len(t, got, len(expected))
	for i := range expected {
		require.Truef(t, proto.Equal(expected[i], got[i]), "message %d mismatch", i)
	}
}

func TestProtobufIteratorNonConcreteType(t *testing.T) {
	data, err := proto.Marshal(timestamppb.New(time.Unix(1718000000, 0).UTC()))
	require.NoError(t, err)

	reader := &messagesReader{
		messages: []*topicreader.Message{messageWithData(t, data)},
	}

	var iterErr error
	for _, err := range ProtobufIterator[proto.Message](context.Background(), reader) {
		if err != nil {
			iterErr = err

			break
		}
	}

	require.Error(t, iterErr)
}

func TestProtobufIteratorMalformedData(t *testing.T) {
	// 0x08 is the wire tag for field 1 as a varint, with no value following:
	// proto.Unmarshal must fail rather than silently produce a message.
	reader := &messagesReader{
		messages: []*topicreader.Message{messageWithData(t, []byte{0x08})},
	}

	var (
		iterErr error
		yielded int
	)
	for msg, err := range ProtobufIterator[*timestamppb.Timestamp](context.Background(), reader) {
		yielded++
		if err != nil {
			iterErr = err

			break
		}
		_ = msg
	}

	require.Error(t, iterErr)
	require.NotErrorIs(t, iterErr, io.EOF)
	require.Equal(t, 1, yielded, "iterator must stop after the unmarshal error")
}
