package topicreader_test

import (
	"context"
	"encoding/binary"
	"errors"
)

type MyMessage struct {
	ID         byte
	ChangeType byte
	Delta      uint32
}

func (m *MyMessage) UnmarshalYDBTopicMessage(data []byte) error {
	if len(data) != 6 {
		return errors.New("bad data len")
	}
	m.ID = data[0]
	m.ChangeType = data[1]
	m.Delta = binary.BigEndian.Uint32(data[2:])
	return nil
}

func Example_effectiveUnmarshalMessageContentToOwnType() {
	ctx := context.TODO()
	reader := readerConnect()

	var v MyMessage
	mess, _ := reader.ReadMessage(ctx)
	_ = mess.UnmarshalTo(&v)
}
