package tracefuzz

import "reflect"

// InvokeAll calls every hook on a trace struct and recursively invokes returned callbacks.
func InvokeAll(v reflect.Value, f *Fuzzer) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		callFunc(v.Field(i), t.Field(i).Type, f)
	}
}

func callFunc(fn reflect.Value, ft reflect.Type, f *Fuzzer) {
	if ft.Kind() != reflect.Func || fn.IsNil() {
		return
	}

	args := make([]reflect.Value, ft.NumIn())
	for j := range args {
		argType := ft.In(j)
		arg := reflect.New(argType).Elem()
		Fill(f, arg)
		fillTopicInterfaces(f, arg)
		args[j] = arg
	}

	for _, ret := range fn.Call(args) {
		switch ret.Kind() {
		case reflect.Func:
			callFunc(ret, ret.Type(), f)
		case reflect.Struct:
			InvokeAll(ret, f)
		}
	}
}

func fillTopicInterfaces(f *Fuzzer, v reflect.Value) {
	if v.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanSet() {
			continue
		}
		if field.Kind() != reflect.Interface || field.Type().NumMethod() == 0 {
			continue
		}
		if !field.IsZero() {
			continue
		}
		if val, ok := topicInterface(f, field.Type()); ok && val.IsValid() {
			field.Set(val)
		}
	}
}

func topicInterface(f *Fuzzer, t reflect.Type) (reflect.Value, bool) {
	s := f.String()
	name := t.Name()
	switch name {
	case "TopicReaderStreamSendCommitMessageStartMessageInfo":
		switch f.Choice(3) {
		case 0:
			return reflect.Value{}, true
		case 1:
			var m *fuzzTopicCommitMessage

			return reflect.ValueOf(m), true
		default:
			return reflect.ValueOf(&fuzzTopicCommitMessage{commits: nil}), true
		}
	case "TopicReaderDataResponseInfo":
		switch f.Choice(3) {
		case 0:
			return reflect.Value{}, true
		case 1:
			var r *fuzzTopicDataResponse

			return reflect.ValueOf(r), true
		default:
			return reflect.ValueOf(&fuzzTopicDataResponse{bytesSize: f.Intn(100)}), true
		}
	case "TopicReadStreamInitRequestInfo":
		switch f.Choice(3) {
		case 0:
			return reflect.Value{}, true
		case 1:
			var r *fuzzTopicInitRequest

			return reflect.ValueOf(r), true
		default:
			return reflect.ValueOf(&fuzzTopicInitRequest{consumer: s, topics: []string{s}}), true
		}
	case "TopicWriterResultMessagesInfoAcks":
		switch f.Choice(3) {
		case 0:
			return reflect.Value{}, true
		case 1:
			var a *fuzzTopicWriterAcks

			return reflect.ValueOf(a), true
		default:
			return reflect.ValueOf(&fuzzTopicWriterAcks{count: f.Intn(10)}), true
		}
	default:
		return reflect.Value{}, false
	}
}
