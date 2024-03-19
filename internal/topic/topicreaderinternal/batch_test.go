package topicreaderinternal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatch_New(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		session := &partitionSession{}
		m1 := &PublicMessage{
			commitRange: commitRange{commitOffsetStart: 1, commitOffsetEnd: 2, partitionSession: session},
		}
		m2 := &PublicMessage{
			commitRange: commitRange{commitOffsetStart: 2, commitOffsetEnd: 3, partitionSession: session},
		}
		batch, err := newBatch(session, []*PublicMessage{m1, m2})
		require.NoError(t, err)

		expected := &PublicBatch{
			Messages:    []*PublicMessage{m1, m2},
			commitRange: commitRange{commitOffsetStart: 1, commitOffsetEnd: 3, partitionSession: session},
		}
		require.Equal(t, expected, batch)
	})
}

func TestBatch_Cut(t *testing.T) {
	t.Run("Full", func(t *testing.T) {
		session := &partitionSession{}
		batch, _ := newBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(100)

		require.Equal(t, batch, head)
		require.True(t, rest.isEmpty())
	})
	t.Run("Zero", func(t *testing.T) {
		session := &partitionSession{}
		batch, _ := newBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(0)

		require.Equal(t, batch, rest)
		require.True(t, head.isEmpty())
	})
	t.Run("Middle", func(t *testing.T) {
		session := &partitionSession{}
		batch, _ := newBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(1)

		expectedBatchHead, _ := newBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})
		expectedBatchRest, _ := newBatch(session, []*PublicMessage{{WrittenAt: testTime(2)}})
		require.Equal(t, expectedBatchHead, head)
		require.Equal(t, expectedBatchRest, rest)
	})
}

func TestBatch_Extend(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		session := &partitionSession{}
		m1 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			commitRange: commitRange{commitOffsetStart: 10, commitOffsetEnd: 11, partitionSession: session},
		}
		m2 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			commitRange: commitRange{commitOffsetStart: 11, commitOffsetEnd: 12, partitionSession: session},
		}

		b1 := &PublicBatch{
			Messages:    []*PublicMessage{m1},
			commitRange: m1.commitRange,
		}

		b2 := &PublicBatch{
			Messages:    []*PublicMessage{m2},
			commitRange: m2.commitRange,
		}
		res, err := b1.append(b2)
		require.NoError(t, err)

		expected := &PublicBatch{
			Messages:    []*PublicMessage{m1, m2},
			commitRange: commitRange{commitOffsetStart: 10, commitOffsetEnd: 12, partitionSession: session},
		}
		require.Equal(t, expected, res)
	})
	t.Run("BadInterval", func(t *testing.T) {
		m1 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			commitRange: commitRange{commitOffsetStart: 10, commitOffsetEnd: 11},
		}
		m2 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			commitRange: commitRange{commitOffsetStart: 20, commitOffsetEnd: 30},
		}

		b1 := &PublicBatch{
			Messages:    []*PublicMessage{m1},
			commitRange: m1.commitRange,
		}

		b2 := &PublicBatch{
			Messages:    []*PublicMessage{m2},
			commitRange: m2.commitRange,
		}
		res, err := b1.append(b2)
		require.Error(t, err)

		require.Nil(t, res)
	})
	t.Run("BadSession", func(t *testing.T) {
		session1 := &partitionSession{}
		session2 := &partitionSession{}

		m1 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			commitRange: commitRange{commitOffsetStart: 10, commitOffsetEnd: 11, partitionSession: session1},
		}
		m2 := &PublicMessage{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			commitRange: commitRange{commitOffsetStart: 11, commitOffsetEnd: 12, partitionSession: session2},
		}

		b1 := &PublicBatch{
			Messages:    []*PublicMessage{m1},
			commitRange: m1.commitRange,
		}

		b2 := &PublicBatch{
			Messages:    []*PublicMessage{m2},
			commitRange: m2.commitRange,
		}
		res, err := b1.append(b2)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

func TestSplitBytesByBatches(t *testing.T) {
	checkTotalBytes := func(t *testing.T, totalBytes int, batches ...*PublicBatch) {
		t.Helper()
		sum := 0
		for _, batch := range batches {
			for _, msg := range batch.Messages {
				sum += msg.bufferBytesAccount
			}
		}

		require.Equal(t, totalBytes, sum)
	}

	t.Run("Empty", func(t *testing.T) {
		require.NoError(t, splitBytesByMessagesInBatches(nil, 0))
	})
	t.Run("BytesToNoMessages", func(t *testing.T) {
		require.Error(t, splitBytesByMessagesInBatches(nil, 10))
	})
	t.Run("MetadataOnlyEqually", func(t *testing.T) {
		totalBytes := 30
		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, totalBytes))

		for _, msg := range batch.Messages {
			require.Equal(t, 10, msg.bufferBytesAccount)
		}
		checkTotalBytes(t, totalBytes, batch)
	})
	t.Run("MetadataOnlyWithReminder", func(t *testing.T) {
		totalBytes := 5
		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, 5))

		require.Equal(t, 2, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 2, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 1, batch.Messages[2].bufferBytesAccount)
		checkTotalBytes(t, totalBytes, batch)
	})
	t.Run("OnlyData", func(t *testing.T) {
		totalBytes := 30
		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 10
		}

		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, totalBytes))
		require.Equal(t, 10, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
		checkTotalBytes(t, totalBytes, batch)
	})
	t.Run("DataAndMetadataEqually", func(t *testing.T) {
		totalBytes := 30
		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 5
		}

		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, totalBytes))
		require.Equal(t, 10, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
		checkTotalBytes(t, totalBytes, batch)
	})
	t.Run("DataAndMetadataEquallyTwoBatches", func(t *testing.T) {
		totalBytes := 30
		batch1, err := newBatch(nil, []*PublicMessage{{}, {}})
		require.NoError(t, err)
		batch1.Messages[0].rawDataLen = 5
		batch1.Messages[1].rawDataLen = 5
		batch2, err := newBatch(nil, []*PublicMessage{{}})
		require.NoError(t, err)
		batch2.Messages[0].rawDataLen = 5

		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch1, batch2}, totalBytes))
		require.Equal(t, 10, batch1.Messages[0].bufferBytesAccount)
		require.Equal(t, 10, batch1.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch2.Messages[0].bufferBytesAccount)
		checkTotalBytes(t, totalBytes, batch1, batch2)
	})
	t.Run("DataAndMetadataWithReminder", func(t *testing.T) {
		totalBytes := 32
		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 5
		}

		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, totalBytes))
		require.Equal(t, 11, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 11, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
		checkTotalBytes(t, totalBytes, batch)
	})
	t.Run("BytesSmallerThenCalcedData", func(t *testing.T) {
		totalBytes := 2

		batch, err := newBatch(nil, []*PublicMessage{{}, {}, {}})
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 5
		}

		require.NoError(t, splitBytesByMessagesInBatches([]*PublicBatch{batch}, totalBytes))

		summ := 0
		for _, msg := range batch.Messages {
			summ += msg.bufferBytesAccount
		}
		checkTotalBytes(t, totalBytes, batch)
	})
}

func testTime(num int) time.Time {
	return time.Date(2022, 6, 17, 0, 0, 0, num, time.UTC)
}
