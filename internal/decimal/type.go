package decimal

import (
	"database/sql"
	"math/big"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	_ sql.Scanner = (*Decimal)(nil)
	_ Interface   = (*Decimal)(nil)
)

type (
	Interface interface {
		Decimal() (bytes [16]byte, precision uint32, scale uint32)
	}
	Decimal struct {
		Bytes     [16]byte
		Precision uint32
		Scale     uint32
	}
)

func ToDecimal(v Interface) *Decimal {
	var d Decimal

	d.Bytes, d.Precision, d.Scale = v.Decimal()

	return &d
}

func (d *Decimal) Decimal() (bytes [16]byte, precision uint32, scale uint32) {
	return d.Bytes, d.Precision, d.Scale
}

func (d *Decimal) Scan(value any) error {
	if err := d.apply(value); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d Decimal) String() string {
	v := FromInt128(d.Bytes, d.Precision)

	return Format(v, d.Precision, d.Scale, false)
}

func (d Decimal) Format(trimTrailingZeros bool) string {
	v := FromInt128(d.Bytes, d.Precision)

	return Format(v, d.Precision, d.Scale, trimTrailingZeros)
}

func (d Decimal) BigInt() *big.Int {
	return FromInt128(d.Bytes, d.Precision)
}
