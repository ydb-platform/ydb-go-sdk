package version

const (
	Major = "3"
	Minor = "100"
	Patch = "2"

	Package = "ydb-go-sdk"
)

const (
	Version     = Major + "." + Minor + "." + Patch
	FullVersion = Package + "/" + Version
)
