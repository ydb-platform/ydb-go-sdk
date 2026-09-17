package main

import (
	"reflect"
	"testing"
)

func TestExpandURLsCreatesOneWorkItemPerURL(t *testing.T) {
	got := expandURLs([]string{
		"https://one.example https://two.example",
		"  https://three.example  ",
	})
	want := []string{
		"https://one.example",
		"https://two.example",
		"https://three.example",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expandURLs() = %#v, want %#v", got, want)
	}
}

func TestServiceCacheReusesInitializedService(t *testing.T) {
	var cache serviceCache
	want := &service{}
	initializations := 0
	initService := func() (*service, error) {
		initializations++

		return want, nil
	}

	first, err := cache.get(initService)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	second, err := cache.get(initService)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if first != want || second != want {
		t.Fatalf("cache returned %p and %p, want %p", first, second, want)
	}
	if initializations != 1 {
		t.Fatalf("initializer called %d times, want 1", initializations)
	}
}
