package topicreaderinternal

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

type serverMessageWrapper struct {
	message rawtopicreader.ServerMessage
}

func (w serverMessageWrapper) Type() string {
	if w.message == nil {
		return "<nil>"
	}
	return reflect.TypeOf(w.message).String()
}

func (w serverMessageWrapper) JSONData() io.Reader {
	if w.message == nil {
		return strings.NewReader("null")
	}
	content, _ := json.MarshalIndent(w.message, "", "  ")
	reader := newOneTimeReader(bytes.NewReader(content))
	return &reader
}

func (w serverMessageWrapper) IsReadStreamServerMessageDebugInfo() {}

type clientMessageWrapper struct {
	message rawtopicreader.ClientMessage
}

func (w clientMessageWrapper) Type() string {
	return reflect.TypeOf(w.message).String()
}

func (w clientMessageWrapper) JSONData() io.Reader {
	content, _ := json.MarshalIndent(w.message, "", "  ")
	reader := newOneTimeReader(bytes.NewReader(content))
	return &reader
}

func (w clientMessageWrapper) IsReadStreamClientMessageDebugInfo() {}
