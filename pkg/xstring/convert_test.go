package xstring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesToString(t *testing.T) {
	data := []byte("Hello world!")
	got := FromBytes(data)
	want := string(data)

	assert.Equal(t, want, got)
}

func TestStringToBytes(t *testing.T) {
	data := "Hello world!"
	got := ToBytes(data)
	want := []byte(data)

	assert.Equal(t, want, got)
}
