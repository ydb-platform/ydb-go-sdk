package scheme

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"google.golang.org/protobuf/proto"
)

func permissions(p Permissions) *Ydb_Scheme.Permissions {
	var y Ydb_Scheme.Permissions
	p.To(&y)

	return &y
}

type permissionsDesc interface {
	SetClear(clear bool)
	AppendAction(action *Ydb_Scheme.PermissionsAction)
}

type PermissionsOption func(permissionsDesc)

func WithClearPermissions() PermissionsOption {
	return func(p permissionsDesc) {
		p.SetClear(true)
	}
}

func WithGrantPermissions(p Permissions) PermissionsOption {
	return func(d permissionsDesc) {
		d.AppendAction(Ydb_Scheme.PermissionsAction_builder{
			Grant: proto.ValueOrDefault(permissions(p)),
		}.Build())
	}
}

func WithRevokePermissions(p Permissions) PermissionsOption {
	return func(d permissionsDesc) {
		d.AppendAction(Ydb_Scheme.PermissionsAction_builder{
			Revoke: proto.ValueOrDefault(permissions(p)),
		}.Build())
	}
}

func WithSetPermissions(p Permissions) PermissionsOption {
	return func(d permissionsDesc) {
		d.AppendAction(Ydb_Scheme.PermissionsAction_builder{
			Set: proto.ValueOrDefault(permissions(p)),
		}.Build())
	}
}

func WithChangeOwner(owner string) PermissionsOption {
	return func(d permissionsDesc) {
		d.AppendAction(Ydb_Scheme.PermissionsAction_builder{
			ChangeOwner: proto.String(owner),
		}.Build())
	}
}
