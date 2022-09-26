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
		v := v
		t.Run(v.testName, func(t *testing.T) {
			d := TopicDescription{}
			d.FromRaw(v.rawTopicDescription)
			if !reflect.DeepEqual(d, v.expectedDescription) {
				t.Errorf("got\n%+v\nexpected\n %+v", d, v.expectedDescription)
			}
		})
	}
}
