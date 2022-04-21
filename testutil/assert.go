package testutil

import (
	"reflect"
	"testing"
)

func Equal(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Not equal:\n\tactual: %#v\n\texpected: %#v", actual, expected)
	}
}

func NoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Received unexpected error: %+v", err)
	}
}

func NotNil(t *testing.T, value interface{}) {
	t.Helper()
	if value == nil {
		t.Fatal("value is nil")
	}
}
