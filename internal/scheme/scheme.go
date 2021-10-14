package scheme

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
)

type Client interface {
	Scheme

	CleanupDatabase(ctx context.Context, prefix string, names ...string) error
	EnsurePathExists(ctx context.Context, path string) error
}

type Scheme interface {
	DescribePath(ctx context.Context, path string) (e Entry, err error)
	MakeDirectory(ctx context.Context, path string) (err error)
	ListDirectory(ctx context.Context, path string) (d Directory, err error)
	RemoveDirectory(ctx context.Context, path string) (err error)

	Close(ctx context.Context) error
}

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
		return "Name"
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

func (e *Entry) IsDirectory() bool {
	return e.Type == EntryDirectory
}
func (e *Entry) IsTable() bool {
	return e.Type == EntryTable
}
func (e *Entry) IsPersQueueGroup() bool {
	return e.Type == EntryPersQueueGroup
}
func (e *Entry) IsDatabase() bool {
	return e.Type == EntryDatabase
}
func (e *Entry) IsRtmrVolume() bool {
	return e.Type == EntryRtmrVolume
}
func (e *Entry) IsBlockStoreVolume() bool {
	return e.Type == EntryBlockStoreVolume
}
func (e *Entry) IsCoordinationNode() bool {
	return e.Type == EntryCoordinationNode
}

type Directory struct {
	Entry
	Children []Entry
}

type client struct {
	cluster cluster.Cluster
	service Ydb_Scheme_V1.SchemeServiceClient
}

func (c *client) Close(_ context.Context) error {
	return nil
}

func New(c cluster.Cluster) Scheme {
	return &client{
		cluster: c,
		service: Ydb_Scheme_V1.NewSchemeServiceClient(c),
	}
}

func (c *client) MakeDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.MakeDirectory(ctx, &Ydb_Scheme.MakeDirectoryRequest{
		Path: path,
	})
	return err
}

func (c *client) RemoveDirectory(ctx context.Context, path string) (err error) {
	_, err = c.service.RemoveDirectory(ctx, &Ydb_Scheme.RemoveDirectoryRequest{
		Path: path,
	})
	return err
}

func (c *client) ListDirectory(ctx context.Context, path string) (Directory, error) {
	var (
		d        Directory
		err      error
		response *Ydb_Scheme.ListDirectoryResponse
		result   Ydb_Scheme.ListDirectoryResult
	)
	response, err = c.service.ListDirectory(ctx, &Ydb_Scheme.ListDirectoryRequest{
		Path: path,
	})
	if err != nil {
		return d, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return d, err
	}
	d.from(result.Self)
	d.Children = make([]Entry, len(result.Children))
	putEntry(d.Children, result.Children)
	return d, nil
}

func (c *client) DescribePath(ctx context.Context, path string) (e Entry, err error) {
	var (
		response *Ydb_Scheme.DescribePathResponse
		result   Ydb_Scheme.DescribePathResult
	)
	response, err = c.service.DescribePath(ctx, &Ydb_Scheme.DescribePathRequest{
		Path: path,
	})
	if err != nil {
		return e, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return e, err
	}
	e.from(result.Self)
	return e, nil
}

func (c *client) ModifyPermissions(ctx context.Context, path string, opts ...PermissionsOption) (err error) {
	var desc permissionsDesc
	for _, opt := range opts {
		opt(&desc)
	}
	_, err = c.service.ModifyPermissions(ctx, &Ydb_Scheme.ModifyPermissionsRequest{
		Path:             path,
		Actions:          desc.actions,
		ClearPermissions: desc.clear,
	})
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
