package querytimestamp

import (
	"cmp"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

var (
	ErrDifferentDatabaseIdentity = errors.New("cannot compare virtual timestamps from different database identities")
	ErrUnknownDatabase           = errors.New("cannot compare virtual timestamps without database identity")
)

// VirtualTimestamp is a Query-client-scoped commit timestamp. Its zero value has no database identity.
type VirtualTimestamp struct {
	planStep uint64
	txID     uint64
	identity *Identity
}

// Identity scopes timestamps to one Query client configuration.
type Identity struct {
	database string
}

func NewIdentity(database string) *Identity {
	return &Identity{database: database}
}

func FromYDB(value *Ydb.VirtualTimestamp, identity *Identity) *VirtualTimestamp {
	if value == nil {
		return nil
	}

	return &VirtualTimestamp{
		planStep: value.GetPlanStep(),
		txID:     value.GetTxId(),
		identity: identity,
	}
}

func (t VirtualTimestamp) PlanStep() uint64 { return t.planStep }
func (t VirtualTimestamp) TxID() uint64     { return t.txID }
func (t VirtualTimestamp) Database() string {
	if t.identity == nil {
		return ""
	}

	return t.identity.database
}

// Compare orders timestamps by plan step and then transaction ID.
// Timestamps without a database identity or from different Query clients cannot be compared.
func (t VirtualTimestamp) Compare(other VirtualTimestamp) (int, error) {
	if t.Database() == "" || other.Database() == "" {
		return 0, ErrUnknownDatabase
	}
	if t.identity != other.identity {
		return 0, ErrDifferentDatabaseIdentity
	}
	if result := cmp.Compare(t.planStep, other.planStep); result != 0 {
		return result, nil
	}

	return cmp.Compare(t.txID, other.txID), nil
}
