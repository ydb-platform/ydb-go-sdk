package experimental

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Experimental_V1"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Experimental"
	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"google.golang.org/protobuf/proto"
)

type client struct {
	client Ydb_Experimental_V1.ExperimentalServiceClient
}

func New(cluster ydb.Cluster) *client {
	return &client{
		client: Ydb_Experimental_V1.NewExperimentalServiceClient(cluster),
	}
}

// GetDiskSpaceUsage return size database statistics
func (c *client) GetDiskSpaceUsage(ctx context.Context, database string) (*DiskSpaceResult, error) {
	response, err := c.client.GetDiskSpaceUsage(
		ctx,
		&Ydb_Experimental.GetDiskSpaceUsageRequest{
			OperationParams: nil,
			Database:        database,
		},
	)
	if err != nil {
		return nil, err
	}

	var getDiskSpaceUsageResult Ydb_Experimental.GetDiskSpaceUsageResult
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &getDiskSpaceUsageResult)
	if err != nil {
		return nil, err
	}

	return &DiskSpaceResult{
		CloudID:    getDiskSpaceUsageResult.CloudId,
		FolderID:   getDiskSpaceUsageResult.FolderId,
		DatabaseID: getDiskSpaceUsageResult.DatabaseId,
		TotalSize:  getDiskSpaceUsageResult.TotalSize,
		DataSize:   getDiskSpaceUsageResult.DataSize,
		IndexSize:  getDiskSpaceUsageResult.IndexSize,
	}, nil
}

type DiskSpaceResult struct {
	CloudID    string
	FolderID   string
	DatabaseID string
	TotalSize  uint64
	DataSize   uint64
	IndexSize  uint64
}
