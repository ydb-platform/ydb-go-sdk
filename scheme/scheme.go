package scheme

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Scheme_V1"
	"google.golang.org/protobuf/proto"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Scheme"
	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
)

type EntryType uint

const (
	EntryTypeUnknown EntryType = iota
	EntryDirectory
	EntryTable
	EntryPersQueueGroup
	EntryDatabase
	EntryRtmrVolume
	EntryBlockStoreVolume
	EntryCoordinationNode
)

func (t EntryType) String() string {
	switch t {
	default:
		return "Unknown"
	case EntryDirectory:
		return "Directory"
	case EntryTable:
		return "Table"
	case EntryPersQueueGroup:
		return "PersQueueGroup"
	case EntryDatabase:
		return "Database"
	case EntryRtmrVolume:
		return "RtmrVolume"
	case EntryBlockStoreVolume:
		return "BlockStoreVolume"
	case EntryCoordinationNode:
		return "CoordinationNode"
	}
}

type Permissions struct {
	Subject         string
	PermissionNames []string
}

type Entry struct {
	Name                 string
	Owner                string
	Type                 EntryType
	Permissions          []Permissions
	EffectivePermissions []Permissions
}

type Directory struct {
	Entry
	Children []Entry
}

type Client struct {
	schemeService Ydb_Scheme_V1.SchemeServiceClient
}

func NewClient(cluster ydb.Cluster) *Client {
	return &Client{
		schemeService: Ydb_Scheme_V1.NewSchemeServiceClient(cluster.GetLazy()),
	}
}

func (c *Client) MakeDirectory(ctx context.Context, path string) (err error) {
	request := Ydb_Scheme.MakeDirectoryRequest{
		Path: path,
	}
	_, err = c.schemeService.MakeDirectory(ctx, &request)
	return err
}

func (c *Client) RemoveDirectory(ctx context.Context, path string) (err error) {
	request := Ydb_Scheme.RemoveDirectoryRequest{
		Path: path,
	}
	_, err = c.schemeService.RemoveDirectory(ctx, &request)
	return err
}

func (c *Client) ListDirectory(ctx context.Context, path string) (d Directory, err error) {
	var (
		response            *Ydb_Scheme.ListDirectoryResponse
		listDirectoryResult Ydb_Scheme.ListDirectoryResult
	)
	request := Ydb_Scheme.ListDirectoryRequest{
		Path: path,
	}
	response, err = c.schemeService.ListDirectory(ctx, &request)
	if err != nil {
		return d, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &listDirectoryResult)
	if err != nil {
		return d, err
	}
	d.Entry.from(listDirectoryResult.Self)
	d.Children = make([]Entry, len(listDirectoryResult.Children))
	putEntry(d.Children, listDirectoryResult.Children)
	return d, nil
}

func (c *Client) DescribePath(ctx context.Context, path string) (e Entry, err error) {
	var (
		response           *Ydb_Scheme.DescribePathResponse
		describePathResult Ydb_Scheme.DescribePathResult
	)
	request := Ydb_Scheme.DescribePathRequest{
		Path: path,
	}
	response, err = c.schemeService.DescribePath(ctx, &request)
	if err != nil {
		return e, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &describePathResult)
	if err != nil {
		return e, err
	}
	e.from(describePathResult.Self)
	return e, nil
}

func (c *Client) ModifyPermissions(ctx context.Context, path string, opts ...PermissionsOption) (err error) {
	var desc permissionsDesc
	for _, opt := range opts {
		opt(&desc)
	}
	request := Ydb_Scheme.ModifyPermissionsRequest{
		Path:             path,
		Actions:          desc.actions,
		ClearPermissions: desc.clear,
	}
	_, err = c.schemeService.ModifyPermissions(ctx, &request)
	return err
}

func (e *Entry) from(y *Ydb_Scheme.Entry) {
	var (
		n = len(y.Permissions)
		m = len(y.EffectivePermissions)
		p = make([]Permissions, n+m)
	)
	putPermissions(p[:n], y.Permissions)
	putPermissions(p[n:], y.EffectivePermissions)
	*e = Entry{
		Name:                 y.Name,
		Owner:                y.Owner,
		Type:                 entryType(y.Type),
		Permissions:          p[0:n],
		EffectivePermissions: p[n:m],
	}
}

func entryType(t Ydb_Scheme.Entry_Type) EntryType {
	switch t {
	case Ydb_Scheme.Entry_DIRECTORY:
		return EntryDirectory
	case Ydb_Scheme.Entry_TABLE:
		return EntryTable
	case Ydb_Scheme.Entry_PERS_QUEUE_GROUP:
		return EntryPersQueueGroup
	case Ydb_Scheme.Entry_DATABASE:
		return EntryDatabase
	case Ydb_Scheme.Entry_RTMR_VOLUME:
		return EntryRtmrVolume
	case Ydb_Scheme.Entry_BLOCK_STORE_VOLUME:
		return EntryBlockStoreVolume
	case Ydb_Scheme.Entry_COORDINATION_NODE:
		return EntryCoordinationNode
	default:
		return EntryTypeUnknown
	}
}

func (p Permissions) to(y *Ydb_Scheme.Permissions) {
	y.Subject = p.Subject
	y.PermissionNames = p.PermissionNames
}

func (p *Permissions) from(y *Ydb_Scheme.Permissions) {
	*p = Permissions{
		Subject:         y.Subject,
		PermissionNames: y.PermissionNames,
	}
}

func putEntry(dst []Entry, src []*Ydb_Scheme.Entry) {
	for i, e := range src {
		(dst[i]).from(e)
	}
}

func putPermissions(dst []Permissions, src []*Ydb_Scheme.Permissions) {
	for i, p := range src {
		(dst[i]).from(p)
	}
}

func InnerConvertEntry(y *Ydb_Scheme.Entry) *Entry {
	res := &Entry{}
	res.from(y)
	return res
}
