package xreflect

import "reflect"

func IsContainsNilPointer(v any) bool {
	if v == nil {
		return true
	}

	rVal := reflect.ValueOf(v)

	return isValPointToNil(rVal)
}

func isValPointToNil(v reflect.Value) bool {
	kind := v.Kind()
	var res bool
	switch kind {
	case reflect.Slice:
		return false
	case reflect.Chan, reflect.Func, reflect.Map, reflect.UnsafePointer:
		res = v.IsNil()
	case reflect.Pointer, reflect.Interface:
		elem := v.Elem()
		if v.IsNil() {
			return true
		}
		res = isValPointToNil(elem)
	}

	return res
}
