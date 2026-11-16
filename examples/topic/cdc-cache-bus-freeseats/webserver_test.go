package main

import "testing"

func TestHasAvailableSeatsRejectsZero(t *testing.T) {
	tests := []struct {
		name      string
		freeSeats int64
		want      bool
	}{
		{name: "negative", freeSeats: -1, want: false},
		{name: "zero", freeSeats: 0, want: false},
		{name: "one", freeSeats: 1, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasAvailableSeats(tt.freeSeats); got != tt.want {
				t.Fatalf("hasAvailableSeats(%d) = %v, want %v", tt.freeSeats, got, tt.want)
			}
		})
	}
}
