package version

const (
	Major = "3"
	Minor = "123"
	Patch = "1"

	Package = "ydb-go-sdk"
)

const (
	Version     = Major + "." + Minor + "." + Patch
	FullVersion = Package + "/" + Version
)
