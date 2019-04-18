package scheme

import (
	"context"

	ydb "github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/grpc/Ydb_Scheme_V1"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Scheme"
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
	Driver ydb.Driver
}

func (c *Client) MakeDirectory(ctx context.Context, path string) error {
	req := Ydb_Scheme.MakeDirectoryRequest{
		Path: path,
	}
	return c.Driver.Call(ctx, internal.Wrap(Ydb_Scheme_V1.MakeDirectory, &req, nil))
}

func (c *Client) RemoveDirectory(ctx context.Context, path string) error {
	req := Ydb_Scheme.RemoveDirectoryRequest{
		Path: path,
	}
	return c.Driver.Call(ctx, internal.Wrap(
		Ydb_Scheme_V1.RemoveDirectory, &req, nil,
	))
}

func (c *Client) ListDirectory(ctx context.Context, path string) (d Directory, err error) {
	var res Ydb_Scheme.ListDirectoryResult
	req := Ydb_Scheme.ListDirectoryRequest{
		Path: path,
	}
	err = c.Driver.Call(ctx, internal.Wrap(
		Ydb_Scheme_V1.ListDirectory, &req, &res,
	))
	if err != nil {
		return d, err
	}
	d.Entry.from(res.Self)
	d.Children = make([]Entry, len(res.Children))
	putEntry(d.Children, res.Children)
	return d, nil
}

func (c *Client) DescribePath(ctx context.Context, path string) (e Entry, err error) {
	var res Ydb_Scheme.DescribePathResult
	req := Ydb_Scheme.DescribePathRequest{
		Path: path,
	}
	err = c.Driver.Call(ctx, internal.Wrap(
		Ydb_Scheme_V1.DescribePath, &req, &res,
	))
	if err == nil {
		e.from(res.Self)
	}
	return e, err
}

func (c *Client) ModifyPermissions(ctx context.Context, path string, opts ...PermissionsOption) error {
	var desc permissionsDesc
	for _, opt := range opts {
		opt(&desc)
	}
	req := Ydb_Scheme.ModifyPermissionsRequest{
		Path:             path,
		Actions:          desc.actions,
		ClearPermissions: desc.clear,
	}
	return c.Driver.Call(ctx, internal.Wrap(
		Ydb_Scheme_V1.ModifyPermissions, &req, nil,
	))
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
