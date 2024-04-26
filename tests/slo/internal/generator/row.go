package generator

import "time"

type RowID = uint64

//nolint:tagalign
type Row struct {
	Hash             uint64     `sql:"hash" gorm:"column:hash;primarykey;autoIncrement:false" xorm:"pk 'hash'"`
	ID               RowID      `sql:"id" gorm:"column:id;primarykey;autoIncrement:false" xorm:"pk 'id'"`
	PayloadStr       *string    `sql:"payload_str" gorm:"column:payload_str" xorm:"'payload_str'"`
	PayloadDouble    *float64   `sql:"payload_double" gorm:"column:payload_double" xorm:"'payload_double'"`
	PayloadTimestamp *time.Time `sql:"payload_timestamp" gorm:"column:payload_timestamp" xorm:"'payload_timestamp'"`
	PayloadHash      uint64     `sql:"payload_hash" gorm:"column:payload_hash" xorm:"'payload_hash'"`
}
