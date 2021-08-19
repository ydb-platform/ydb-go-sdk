package experimental

import (
	"bytes"
	"context"
	"github.com/YandexDatabase/ydb-go-sdk/v2"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Experimental"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
)

type Client struct {
	Driver ydb.Driver
}

// UploadRows not fully supported yet
func (c *Client) UploadRows(ctx context.Context, tableName string, typ ydb.Value) (err error) {
	req := Ydb_Experimental.UploadRowsRequest{
		Table:           tableName,
		Rows:            internal.ValueToYDB(typ),
		OperationParams: nil,
	}
	_, err = c.Driver.Call(ctx, ydb.Wrap("/Ydb.Experimental.V1.ExperimentalService/UploadRows", &req, nil))
	return
}

// ExecuteStreamQuery not fully supported yet
func (c *Client) ExecuteStreamQuery(ctx context.Context, query string, params *QueryParameters, opts ...StreamQueryOption) (*StreamQueryResult, error) {
	var res Ydb_Experimental.ExecuteStreamQueryResult
	req := Ydb_Experimental.ExecuteStreamQueryRequest{
		YqlText:    query,
		Parameters: params.params(),
	}
	for _, opt := range opts {
		opt((*streamQueryDesc)(&req))
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap("/Ydb.Experimental.V1.ExperimentalService/ExecuteStreamQuery", &req, &res))
	if err != nil {
		return nil, err
	}
	return streamQueryResult(&res), nil
}

// GetDiskSpaceUsage return size database statistics
func (c *Client) GetDiskSpaceUsage(ctx context.Context, database string) (*DiskSpaceResult, error) {
	var res Ydb_Experimental.GetDiskSpaceUsageResult
	req := Ydb_Experimental.GetDiskSpaceUsageRequest{
		OperationParams: nil,
		Database:        database,
	}
	_, err := c.Driver.Call(ctx, ydb.Wrap("/Ydb.Experimental.V1.ExperimentalService/GetDiskSpaceUsage", &req, &res))
	if err != nil {
		return nil, err
	}

	return &DiskSpaceResult{
		CloudID:    res.CloudId,
		FolderID:   res.FolderId,
		DatabaseID: res.DatabaseId,
		TotalSize:  res.TotalSize,
		DataSize:   res.DataSize,
		IndexSize:  res.IndexSize,
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

// QueryParameters TODO: unite with table query parameters
type QueryParameters struct {
	m queryParams
}

func (q *QueryParameters) params() queryParams {
	if q == nil {
		return nil
	}
	return q.m
}

func (q *QueryParameters) Each(it func(name string, value ydb.Value)) {
	if q == nil {
		return
	}
	for key, value := range q.m {
		it(key, internal.ValueFromYDB(
			value.Type,
			value.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, value ydb.Value) {
		buf.WriteString("((")
		buf.WriteString(name)
		buf.WriteString(")(")
		internal.WriteValueStringTo(&buf, value)
		buf.WriteString("))")
	})
	buf.WriteByte(')')
	return buf.String()
}

type queryParams map[string]*Ydb.TypedValue

type ParameterOption func(queryParams)

func NewQueryParameters(opts ...ParameterOption) *QueryParameters {
	q := &QueryParameters{
		m: make(queryParams, len(opts)),
	}
	q.Add(opts...)
	return q
}

func (q *QueryParameters) Add(opts ...ParameterOption) {
	for _, opt := range opts {
		opt(q.m)
	}
}

func ValueParam(name string, v ydb.Value) ParameterOption {
	return func(q queryParams) {
		q[name] = internal.ValueToYDB(v)
	}
}
