package balancer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn/state"
	"testing"
	"time"
)

func TestAbsDuration(t *testing.T) {
	for _, test := range []struct {
		d time.Duration
	}{
		{
			d: time.Minute,
		},
		{
			d: -time.Minute,
		},
	} {
		t.Run(test.d.String(), func(t *testing.T) {
			abs := absDuration(test.d)
			switch {
			case test.d >= 0 && abs+test.d == 2*test.d:
				return
			case abs+test.d == 0:
				return
			}
			t.Errorf("unexpected abs value")
		})
	}
}

func TestChooseByState(t *testing.T) {
	c := connRuntimeCriterion{}
	c1, c2 := &cluster.connListElement{
		index: 1,
	}, &cluster.connListElement{
		index: 2,
	}
	for _, test := range [...]struct {
		name   string
		s1     state.State
		s2     state.State
		choice *cluster.connListElement
	}{
		{
			s1:     state.Unknown,
			s2:     state.Unknown,
			choice: nil,
		},
		{
			s1:     state.Unknown,
			s2:     state.Online,
			choice: c2,
		},
		{
			s1:     state.Online,
			s2:     state.Unknown,
			choice: c1,
		},
		{
			s1:     state.Unknown,
			s2:     state.Offline,
			choice: c1,
		},
		{
			s1:     state.Offline,
			s2:     state.Unknown,
			choice: c2,
		},
		{
			s1:     state.Unknown,
			s2:     state.Banned,
			choice: c1,
		},
		{
			s1:     state.Banned,
			s2:     state.Unknown,
			choice: c2,
		},
		{
			s1:     state.Online,
			s2:     state.Online,
			choice: nil,
		},
		{
			s1:     state.Online,
			s2:     state.Offline,
			choice: c1,
		},
		{
			s1:     state.Online,
			s2:     state.Banned,
			choice: c1,
		},
		{
			s1:     state.Offline,
			s2:     state.Online,
			choice: c2,
		},
		{
			s1:     state.Banned,
			s2:     state.Online,
			choice: c2,
		},
		{
			s1:     state.Offline,
			s2:     state.Offline,
			choice: nil,
		},
		{
			s1:     state.Offline,
			s2:     state.Banned,
			choice: c2,
		},
		{
			s1:     state.Banned,
			s2:     state.Offline,
			choice: c1,
		},
		{
			s1:     state.Banned,
			s2:     state.Banned,
			choice: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if choise := c.chooseByState(c1, c2, test.s1, test.s2); choise != test.choice {
				t.Errorf("unexpected choice (%d vs %d): %T(%+v), expected: %T(%+v)", test.s1, test.s2, choise, choise, test.choice, test.choice)
			}
		})
	}
}
