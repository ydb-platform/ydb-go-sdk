package tracefuzz

import "reflect"

func topicInterface(f *Fuzzer, t reflect.Type) (reflect.Value, bool) {
	switch t.Name() {
	case "TopicReaderStreamSendCommitMessageStartMessageInfo":
		return topicCommitMessageInterface(f)
	case "TopicReaderDataResponseInfo":
		return topicDataResponseInterface(f)
	case "TopicReadStreamInitRequestInfo":
		return topicInitRequestInterface(f)
	case "TopicWriterResultMessagesInfoAcks":
		return topicWriterAcksInterface(f)
	default:
		return reflect.Value{}, false
	}
}

func topicCommitMessageInterface(f *Fuzzer) (reflect.Value, bool) {
	switch f.Choice(3) {
	case 0:
		return reflect.Value{}, true
	case 1:
		var m *fuzzTopicCommitMessage

		return reflect.ValueOf(m), true
	default:
		return reflect.ValueOf(&fuzzTopicCommitMessage{commits: nil}), true
	}
}

func topicDataResponseInterface(f *Fuzzer) (reflect.Value, bool) {
	switch f.Choice(3) {
	case 0:
		return reflect.Value{}, true
	case 1:
		var r *fuzzTopicDataResponse

		return reflect.ValueOf(r), true
	default:
		return reflect.ValueOf(&fuzzTopicDataResponse{bytesSize: f.Intn(100)}), true
	}
}

func topicInitRequestInterface(f *Fuzzer) (reflect.Value, bool) {
	s := f.String()
	switch f.Choice(3) {
	case 0:
		return reflect.Value{}, true
	case 1:
		var r *fuzzTopicInitRequest

		return reflect.ValueOf(r), true
	default:
		return reflect.ValueOf(&fuzzTopicInitRequest{consumer: s, topics: []string{s}}), true
	}
}

func topicWriterAcksInterface(f *Fuzzer) (reflect.Value, bool) {
	switch f.Choice(3) {
	case 0:
		return reflect.Value{}, true
	case 1:
		var a *fuzzTopicWriterAcks

		return reflect.ValueOf(a), true
	default:
		return reflect.ValueOf(&fuzzTopicWriterAcks{count: f.Intn(10)}), true
	}
}
