package generator

import "time"

type RowID = uint64

type Row struct {
	ID               RowID      `gorm:"column:id;primarykey;autoIncrement:false"`
	PayloadStr       *string    `gorm:"column:payload_str"`
	PayloadDouble    *float64   `gorm:"column:payload_double"`
	PayloadTimestamp *time.Time `gorm:"column:payload_timestamp"`
}
