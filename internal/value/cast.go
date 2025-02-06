package value

import (
	"database/sql"
	"database/sql/driver"

	"github.com/google/uuid"
)

func CastTo(v Value, dst interface{}) error {
	if dst == nil {
		return errNilDestination
	}
	if ptr, has := dst.(*Value); has {
		*ptr = v

		return nil
	}

	if _, ok := dst.(*uuid.UUID); ok {
		return v.castTo(dst)
	}

	if scanner, has := dst.(sql.Scanner); has {
		dv := new(driver.Value)

		err := v.castTo(dv)
		if err != nil {
			return err
		}

		return scanner.Scan(*dv)
	}

	return v.castTo(dst)
}
