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
		for _, opt := range opts {
			if opt != nil {
				opt(&desc)
			}
		}

		if !desc.clear {
			t.Errorf("Clear is not as expected")
		}

		count := len(desc.actions)
		for _, a := range desc.actions {
			switch a.WhichAction() {
			case Ydb_Scheme.PermissionsAction_ChangeOwner_case:
				count--
				if a.GetChangeOwner() != "ow" {
					t.Errorf("Owner is not as expected")
				}
			case Ydb_Scheme.PermissionsAction_Grant_case:
				count--
				if a.GetGrant().GetSubject() != "grant" || len(a.GetGrant().GetPermissionNames()) != 3 {
					t.Errorf("Grant is not as expected")
				}
			case Ydb_Scheme.PermissionsAction_Set_case:
				count--
				if a.GetSet().GetSubject() != "set" || len(a.GetSet().GetPermissionNames()) != 1 || a.GetSet().GetPermissionNames()[0] != "d" {
					t.Errorf("Set is not as expected")
				}
			case Ydb_Scheme.PermissionsAction_Revoke_case:
				count--
				revokeSubject := a.GetRevoke().GetSubject()
				permissionNames := a.GetRevoke().GetPermissionNames()
				if revokeSubject != "revoke" || len(permissionNames) != 1 || permissionNames[0] != "e" {
					t.Errorf("Revoke is not as expected")
				}
			}
		}

		if count != 0 {
			t.Errorf("Count of permission actions is not as expected")
		}
	}
}
