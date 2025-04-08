package topictypes

import (
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawscheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

var (
	fourPM = time.Date(2024, 0o1, 0o1, 16, 0, 0, 0, time.UTC)
	hour   = time.Hour
)

func TestTopicDescriptionFromRaw(t *testing.T) {
	testData := []struct {
		testName            string
		expectedDescription TopicDescription
		rawTopicDescription *rawtopic.DescribeTopicResult
	}{
		{
			testName: "empty topic description",
			expectedDescription: TopicDescription{
				Attributes:      make(map[string]string),
				Consumers:       make([]Consumer, 0),
				SupportedCodecs: make([]Codec, 0),
				Partitions:      make([]PartitionInfo, 0),
			},
			rawTopicDescription: &rawtopic.DescribeTopicResult{},
		},
		{
			testName: "all fields set",
			expectedDescription: TopicDescription{
				Attributes: map[string]string{
					"privet": "mir",
					"hello":  "world",
				},
				Consumers: []Consumer{
					{
						Name:      "privet mir",
						Important: true,
						Attributes: map[string]string{
							"hello":  "world",
							"privet": "mir",
						},
						SupportedCodecs: []Codec{
							CodecRaw,
							CodecGzip,
						},
						ReadFrom: time.Date(2022, time.March, 8, 12, 12, 12, 0, time.UTC),
					},
				},
				Path: "some/path",
				PartitionSettings: PartitionSettings{
					MinActivePartitions: 4,
					PartitionCountLimit: 4,
				},
				Partitions: []PartitionInfo{
					{
						PartitionID: 42,
						Active:      true,
						ChildPartitionIDs: []int64{
							1, 2, 3,
						},
						ParentPartitionIDs: []int64{
							1, 2, 3,
						},
						PartitionStats: PartitionStats{
							PartitionsOffset: OffsetRange{
								Start: 10,
								End:   20,
							},
							StoreSizeBytes:  1024,
							LastWriteTime:   &fourPM,
							MaxWriteTimeLag: &hour,
							BytesWritten: MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
						},
					},
					{
						PartitionID: 43,
						Active:      false,
						ChildPartitionIDs: []int64{
							42,
						},
						ParentPartitionIDs: []int64{
							47,
						},
					},
				},
				RetentionPeriod:    time.Hour,
				RetentionStorageMB: 1024,
				SupportedCodecs: []Codec{
					CodecRaw,
					CodecGzip,
				},
				PartitionWriteBurstBytes:          1024,
				PartitionWriteSpeedBytesPerSecond: 1024,
				MeteringMode:                      MeteringModeRequestUnits,
			},
			rawTopicDescription: &rawtopic.DescribeTopicResult{
				Self: rawscheme.Entry{
					Name: "some/path",
				},
				PartitioningSettings: rawtopic.PartitioningSettings{
					MinActivePartitions: 4,
					PartitionCountLimit: 4,
				},
				Partitions: []rawtopic.PartitionInfo{
					{
						PartitionID: 42,
						Active:      true,
						ChildPartitionIDs: []int64{
							1, 2, 3,
						},
						ParentPartitionIDs: []int64{
							1, 2, 3,
						},
						PartitionStats: rawtopic.PartitionStats{
							PartitionsOffset: rawtopiccommon.OffsetRange{
								Start: 10,
								End:   20,
							},
							StoreSizeBytes: 1024,
							LastWriteTime: rawoptional.Time{
								Value:    fourPM,
								HasValue: true,
							},
							MaxWriteTimeLag: rawoptional.Duration{
								Value:    hour,
								HasValue: true,
							},
							BytesWritten: rawtopic.MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
						},
					},
					{
						PartitionID: 43,
						Active:      false,
						ChildPartitionIDs: []int64{
							42,
						},
						ParentPartitionIDs: []int64{
							47,
						},
					},
				},
				RetentionPeriod:    time.Hour,
				RetentionStorageMB: 1024,
				SupportedCodecs: rawtopiccommon.SupportedCodecs{
					rawtopiccommon.CodecRaw,
					rawtopiccommon.CodecGzip,
				},
				PartitionWriteSpeedBytesPerSecond: 1024,
				PartitionWriteBurstBytes:          1024,
				Attributes: map[string]string{
					"privet": "mir",
					"hello":  "world",
				},
				Consumers: []rawtopic.Consumer{
					{
						Name:      "privet mir",
						Important: true,
						Attributes: map[string]string{
							"privet": "mir",
							"hello":  "world",
						},
						SupportedCodecs: rawtopiccommon.SupportedCodecs{
							rawtopiccommon.CodecRaw,
							rawtopiccommon.CodecGzip,
						},
						ReadFrom: rawoptional.Time{
							Value:    time.Date(2022, time.March, 8, 12, 12, 12, 0, time.UTC),
							HasValue: true,
						},
					},
				},
				MeteringMode: rawtopic.MeteringModeRequestUnits,
			},
		},
	}

	for _, v := range testData {
		t.Run(
			v.testName, func(t *testing.T) {
				d := TopicDescription{}
				d.FromRaw(v.rawTopicDescription)
				if !reflect.DeepEqual(d, v.expectedDescription) {
					t.Errorf("got\n%+v\nexpected\n %+v", d, v.expectedDescription)
				}
			},
		)
	}
}

func TestTopicConsumerDescriptionFromRaw(t *testing.T) {
	testData := []struct {
		testName               string
		expectedDescription    TopicConsumerDescription
		rawConsumerDescription *rawtopic.DescribeConsumerResult
	}{
		{
			testName: "empty topic consumer description",
			expectedDescription: TopicConsumerDescription{
				Path: "",
				Consumer: Consumer{
					SupportedCodecs: make([]Codec, 0),
				},
				Partitions: make([]DescribeConsumerPartitionInfo, 0),
			},
			rawConsumerDescription: &rawtopic.DescribeConsumerResult{},
		},
		{
			testName: "all fields set",
			expectedDescription: TopicConsumerDescription{
				Path: "topic/consumer",
				Consumer: Consumer{
					Attributes: map[string]string{
						"privet": "mir",
						"hello":  "world",
					},
					Name:      "c1",
					Important: true,
					SupportedCodecs: []Codec{
						CodecRaw,
					},
					ReadFrom: time.Date(2022, time.March, 8, 12, 12, 12, 0, time.UTC),
				},
				Partitions: []DescribeConsumerPartitionInfo{
					{
						PartitionID: 42,
						Active:      true,
						ChildPartitionIDs: []int64{
							1, 2, 3,
						},
						ParentPartitionIDs: []int64{
							1, 2, 3,
						},
						PartitionStats: PartitionStats{
							PartitionsOffset: OffsetRange{
								Start: 10,
								End:   20,
							},
							StoreSizeBytes:  1024,
							LastWriteTime:   &fourPM,
							MaxWriteTimeLag: &hour,
							BytesWritten: MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
						},
						PartitionConsumerStats: PartitionConsumerStats{
							LastReadOffset:                 20,
							CommittedOffset:                10,
							ReadSessionID:                  "session1",
							PartitionReadSessionCreateTime: &fourPM,
							LastReadTime:                   &fourPM,
							MaxReadTimeLag:                 &hour,
							MaxWriteTimeLag:                &hour,
							BytesRead: MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
							ReaderName: "reader1",
						},
					},
				},
			},
			rawConsumerDescription: &rawtopic.DescribeConsumerResult{
				Self: rawscheme.Entry{
					Name: "topic/consumer",
				},
				Consumer: rawtopic.Consumer{
					Name:            "c1",
					Important:       true,
					SupportedCodecs: rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecRaw},
					ReadFrom: rawoptional.Time{
						Value:    time.Date(2022, time.March, 8, 12, 12, 12, 0, time.UTC),
						HasValue: true,
					},
					Attributes: map[string]string{
						"privet": "mir",
						"hello":  "world",
					},
				},
				Partitions: []rawtopic.DescribeConsumerResultPartitionInfo{
					{
						PartitionID: 42,
						Active:      true,
						ChildPartitionIDs: []int64{
							1, 2, 3,
						},
						ParentPartitionIDs: []int64{
							1, 2, 3,
						},
						PartitionStats: rawtopic.PartitionStats{
							PartitionsOffset: rawtopiccommon.OffsetRange{
								Start: 10,
								End:   20,
							},
							StoreSizeBytes: 1024,
							LastWriteTime: rawoptional.Time{
								Value:    fourPM,
								HasValue: true,
							},
							MaxWriteTimeLag: rawoptional.Duration{
								Value:    hour,
								HasValue: true,
							},
							BytesWritten: rawtopic.MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
						},
						PartitionConsumerStats: rawtopic.PartitionConsumerStats{
							LastReadOffset:  20,
							CommittedOffset: 10,
							ReadSessionID:   "session1",
							PartitionReadSessionCreateTime: rawoptional.Time{
								Value:    fourPM,
								HasValue: true,
							},
							LastReadTime: rawoptional.Time{
								Value:    fourPM,
								HasValue: true,
							},
							MaxReadTimeLag: rawoptional.Duration{
								Value:    hour,
								HasValue: true,
							},
							MaxWriteTimeLag: rawoptional.Duration{
								Value:    hour,
								HasValue: true,
							},
							BytesRead: rawtopic.MultipleWindowsStat{
								PerMinute: 1,
								PerHour:   60,
								PerDay:    1440,
							},
							ReaderName: "reader1",
						},
					},
				},
			},
		},
	}
	for _, v := range testData {
		t.Run(
			v.testName, func(t *testing.T) {
				d := TopicConsumerDescription{}
				d.FromRaw(v.rawConsumerDescription)
				if !reflect.DeepEqual(d, v.expectedDescription) {
					t.Errorf("got\n%+v\nexpected\n%+v", d, v.expectedDescription)
				}
			},
		)
	}
}
