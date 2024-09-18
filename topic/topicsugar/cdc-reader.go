package topicsugar

import (
	"encoding/json"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

// YDBCDCItem interface for represent record from table (and cdc event)
// The interface will be removed in the future  (or may be set as optional)
// and replaced by field annotations
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type YDBCDCItem[K any] interface {
	comparable
	ParseCDCKey([]json.RawMessage) (K, error)
	SetPrimaryKey(K)
}

// YDBCDCMessage is typed representation of cdc event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type YDBCDCMessage[T interface {
	YDBCDCItem[Key]
}, Key any] struct {
	Update   T
	NewImage T
	OldImage T
	Key      Key
	Erase    *struct{}
	TS       []uint64
}

// IsErase returns true if the event about erase record
func (c *YDBCDCMessage[T, Key]) IsErase() bool {
	return c.Erase != nil
}

func (c *YDBCDCMessage[T, Key]) UnmarshalJSON(bytes []byte) error {
	var rawItem struct {
		Update   T
		NewImage T
		OldImage T
		Key      []json.RawMessage
		Erase    *struct{}
		TS       []uint64
	}

	err := json.Unmarshal(bytes, &rawItem)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cdcevent for type %T: %w", c, err)
	}

	var tZero T
	key, err := tZero.ParseCDCKey(rawItem.Key)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cdcevent key for type %T: %w", c, err)
	}

	c.Update = rawItem.Update
	c.NewImage = rawItem.NewImage
	c.OldImage = rawItem.OldImage
	c.Key = key
	c.Erase = rawItem.Erase
	c.TS = rawItem.TS

	if c.Update != tZero {
		c.Update.SetPrimaryKey(key)
	}
	if c.OldImage != tZero {
		c.OldImage.SetPrimaryKey(key)
	}
	if c.NewImage != tZero {
		c.NewImage.SetPrimaryKey(key)
	}

	return nil
}

func UnmarsharCDCStream[K any]() xiter.Seq2[YDB]
