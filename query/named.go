package query

import (
	"fmt"
	"reflect"
)

type namedDestination struct {
	name string
	ref  interface{}
}

func (dst namedDestination) Name() string {
	return dst.name
}

func (dst namedDestination) Destination() interface{} {
	return dst.ref
}

func Named(columnName string, destinationValueReference interface{}) namedDestination {
	dst := namedDestination{}
	if columnName == "" {
		panic("columnName must be not empty")
	}
	dst.name = columnName
	v := reflect.TypeOf(destinationValueReference)
	if v.Kind() != reflect.Ptr {
		panic(fmt.Errorf("%T is not reference type", destinationValueReference))
	}
	dst.ref = destinationValueReference

	return dst
}
