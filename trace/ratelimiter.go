package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Ratelimiter specified trace of ratelimiter client activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	Ratelimiter struct{}
)
