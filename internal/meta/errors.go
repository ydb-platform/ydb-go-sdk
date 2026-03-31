package meta

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
)

var (
	errReservedPackageName   = fmt.Errorf("%q reserved", version.Package)
	errWrongFrameworkName    = errors.New("wrong framework name")
	errWrongFrameworkVersion = errors.New("wrong framework version")
)
