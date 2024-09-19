//go:build go1.23

package topicsugar

import (
	"context"
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
	ParseCDCKey(keyFields []json.RawMessage) (K, error)
	SetPrimaryKey(key K)
}

// YDBCDCMessage is typed representation of cdc event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type YDBCDCMessage[T YDBCDCItem[Key], Key any] struct {
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
		Update   T                 `json:"update"`
		NewImage T                 `json:"newImage"`
		OldImage T                 `json:"oldImage"`
		Key      []json.RawMessage `json:"key"`
		Erase    *struct{}         `json:"erase"`
		TS       []uint64          `json:"ts"`
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

func UnmarshalCDCStream[T YDBCDCItem[K], K any](
	ctx context.Context,
	reader TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[YDBCDCMessage[T, K]], error] {
	var unmarshal TypedUnmarshalFunc[*YDBCDCMessage[T, K]] = func(data []byte, dst *YDBCDCMessage[T, K]) error {
		return json.Unmarshal(data, dst)
	}

	return IteratorFunc[YDBCDCMessage[T, K]](ctx, reader, unmarshal)
}
