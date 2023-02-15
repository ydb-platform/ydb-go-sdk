package scheme

import (
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

func TestSchemeOptions(t *testing.T) {
	{
		opts := []scheme.PermissionsOption{
			scheme.WithClearPermissions(),
			scheme.WithChangeOwner("ow"),
			scheme.WithGrantPermissions(scheme.Permissions{
				Subject:         "grant",
				PermissionNames: []string{"a", "b", "c"},
			}),
			scheme.WithSetPermissions(scheme.Permissions{
				Subject:         "set",
				PermissionNames: []string{"d"},
			}),
			scheme.WithRevokePermissions(scheme.Permissions{
				Subject:         "revoke",
				PermissionNames: []string{"e"},
			}),
		}

		var desc permissionsDesc
		for _, o := range opts {
			if o != nil {
				o(&desc)
			}
		}

		if !desc.clear {
			t.Errorf("Clear is not as expected")
		}

		count := len(desc.actions)
		for _, a := range desc.actions {
			switch a := a.Action.(type) {
			case *Ydb_Scheme.PermissionsAction_ChangeOwner:
				count--
				if a.ChangeOwner != "ow" {
					t.Errorf("Owner is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Grant:
				count--
				if a.Grant.Subject != "grant" || len(a.Grant.PermissionNames) != 3 {
					t.Errorf("Grant is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Set:
				count--
				if a.Set.Subject != "set" || len(a.Set.PermissionNames) != 1 || a.Set.PermissionNames[0] != "d" {
					t.Errorf("Set is not as expected")
				}
			case *Ydb_Scheme.PermissionsAction_Revoke:
				count--
				if a.Revoke.Subject != "revoke" || len(a.Revoke.PermissionNames) != 1 || a.Revoke.PermissionNames[0] != "e" {
					t.Errorf("Revoke is not as expected")
				}
			}
		}

		if count != 0 {
			t.Errorf("Count of permission actions is not as expected")
		}
	}
}
