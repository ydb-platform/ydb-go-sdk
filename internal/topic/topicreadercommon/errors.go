package topicreadercommon

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var PublicErrCommitSessionToExpiredSession = xerrors.Wrap(errors.New("ydb: commit to expired session"))
