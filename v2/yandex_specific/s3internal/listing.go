package s3internal

import (
	"context"

	ydb "github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/grpc/draft/Ydb_S3Internal_V1"
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb_S3Internal"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal"
)

// Client contains methods for make S3 specific requests.
type Client struct {
	Driver ydb.Driver
}

// S3Listing makes request for custom method that implements objects and multipart uploads listing for S3
// `tableName` is full table name including path
// `keyPrefix` is yql.Tuple of values for key columns used for filter results
// `pathColumnPrefix` is prefix of object path
// `pathColumnDelimiter` is object's path delimiter for split path to "folders"
// `startAfterKeySuffix` is values for last key columns after which listing should be started
// `maxKeys` is limit of result
// `columnsToReturn` is names of table columns included to result
func (c *Client) S3Listing(ctx context.Context, tableName string, keyPrefix ydb.Value, pathColumnPrefix string,
	pathColumnDelimiter string, startAfterKeySuffix ydb.Value, maxKeys uint32, columnsToReturn []string,
) (*S3ListingResult, error) {
	var res Ydb_S3Internal.S3ListingResult
	req := Ydb_S3Internal.S3ListingRequest{
		TableName:           tableName,
		KeyPrefix:           internal.ValueToYDB(keyPrefix),
		PathColumnPrefix:    pathColumnPrefix,
		PathColumnDelimiter: pathColumnDelimiter,
		StartAfterKeySuffix: internal.ValueToYDB(startAfterKeySuffix),
		MaxKeys:             maxKeys,
		ColumnsToReturn:     columnsToReturn,
	}
	_, err := c.Driver.Call(ctx, internal.Wrap(
		Ydb_S3Internal_V1.S3Listing, &req, &res,
	))
	if err != nil {
		return nil, err
	}

	return &S3ListingResult{res: &res}, nil
}
