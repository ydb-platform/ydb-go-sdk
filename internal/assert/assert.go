package assert

import (
	"fmt"
	"reflect"
	"testing"
)

func Equal(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal(fmt.Sprintf("Not equal: \n"+
			"expected: %#v\n"+
			"actual  : %#v", expected, actual))
	}
}

func NoError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(fmt.Sprintf("Received unexpected error:\n%+v", err))
	}
}

func NotNil(t *testing.T, value interface{}) {
	if value == nil {
		t.Fatal("value is nil")
	}
}
