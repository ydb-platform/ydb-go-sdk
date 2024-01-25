package credentials

type SourceInfoOption string

func (sourceInfo SourceInfoOption) ApplyStaticCredentialsOption(h *Static) {
	h.sourceInfo = string(sourceInfo)
}

func (sourceInfo SourceInfoOption) ApplyAnonymousCredentialsOption(h *Anonymous) {
	h.sourceInfo = string(sourceInfo)
}

func (sourceInfo SourceInfoOption) ApplyAccessTokenCredentialsOption(h *AccessToken) {
	h.sourceInfo = string(sourceInfo)
}

// WithSourceInfo option append to credentials object the source info for reporting source info details on error case
func WithSourceInfo(sourceInfo string) SourceInfoOption {
	return SourceInfoOption(sourceInfo)
}
