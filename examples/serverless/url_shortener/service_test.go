package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestHashDistinguishesKnownFNV32Collision(t *testing.T) {
	first := hash("url-332789")
	second := hash("url-529192")
	if first == second {
		t.Fatalf("different URLs produced the same short hash %q", first)
	}
}

func TestShortLinkValidationRequiresWholeStrongHash(t *testing.T) {
	valid := hash("https://example.com")

	tests := []struct {
		name string
		link string
		want bool
	}{
		{name: "valid", link: valid, want: true},
		{name: "truncated", link: valid[:8], want: false},
		{name: "suffix", link: valid + "x", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isShortCorrect(tt.link); got != tt.want {
				t.Fatalf("isShortCorrect(%q) = %v, want %v", tt.link, got, tt.want)
			}
		})
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

func TestServerlessReusesServiceWithoutClosingItPerRequest(t *testing.T) {
	router := mux.NewRouter()
	router.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	services = serviceCache{service: &service{router: router}}
	t.Cleanup(func() {
		services = serviceCache{}
	})

	for range 2 {
		response := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		Serverless(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("response status = %d, want %d", response.Code, http.StatusNoContent)
		}
	}
}
