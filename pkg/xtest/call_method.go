package xtest

import (
	"reflect"
)

func CallMethod(object any, name string, args ...any) []any {
	method := reflect.ValueOf(object).MethodByName(name)

	inputs := make([]reflect.Value, len(args))

	for i := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}

	output := method.Call(inputs)

	result := make([]any, len(output))

	for i := range output {
		result[i] = output[i].Interface()
	}

	return result
}
