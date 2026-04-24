package sugar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisGlobToLIKE(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		out  string
	}{
		{
			name: "wildcards",
			in:   "a*b?c",
			out:  "a%b_c",
		},
		{
			name: "escape like special chars",
			in:   "%_!",
			out:  "!%!_!!",
		},
		{
			name: "escaped wildcard and backslash",
			in:   `\*\?\\`,
			out:  `*?\`,
		},
		{
			name: "escaped like special chars",
			in:   `\%\_\!`,
			out:  "!%!_!!",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.out, redisGlobToLIKE(tc.in))
		})
	}
}

func TestRedisGlobToRE2Match(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		out  string
	}{
		{
			name: "wildcards",
			in:   "a*b?c",
			out:  "^a.*b.c$",
		},
		{
			name: "character class",
			in:   "k[ab]",
			out:  "^k[ab]$",
		},
		{
			name: "negated character class",
			in:   "k[!ab]",
			out:  "^k[^ab]$",
		},
		{
			name: "escaped special chars",
			in:   `\*\?\.`,
			out:  `^\*\?\.$`,
		},
		{
			name: "escaped backslash",
			in:   `\\`,
			out:  `^\\$`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := redisGlobToRE2Match(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.out, got)
		})
	}
}

func TestRedisGlobToRE2MatchInvalidPattern(t *testing.T) {
	_, err := redisGlobToRE2Match("abc[")
	require.ErrorContains(t, err, "unclosed '['")

	_, err = redisGlobToRE2Match("a[]")
	require.ErrorContains(t, err, "empty character class")
}

func TestRedisKeysPatternToYQL(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		out  listPattern
	}{
		{
			name: "default pattern",
			in:   "",
			out:  listPattern{useMatch: false, like: "%"},
		},
		{
			name: "like pattern",
			in:   `a\*\?%_!`,
			out:  listPattern{useMatch: false, like: "a*?!%!_!!"},
		},
		{
			name: "match pattern",
			in:   "k[!ab]",
			out:  listPattern{useMatch: true, re2: "^k[^ab]$"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := redisKeysPatternToYQL(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.out, got)
		})
	}
}

func TestRedisKeysPatternToYQLInvalidPattern(t *testing.T) {
	_, err := redisKeysPatternToYQL("[abc")
	require.ErrorContains(t, err, "unclosed '['")
}
