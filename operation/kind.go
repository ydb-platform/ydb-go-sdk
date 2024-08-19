package operation

type kind string

const (
	KindScriptExec = kind("scriptexec")
	KindBuildIndex = kind("buildindex")
	KindImportS3   = kind("import/s3")
	KindExportS3   = kind("export/s3")
	KindExportYT   = kind("export/yt")
)
