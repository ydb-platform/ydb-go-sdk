package errors

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type tableError struct {
	error
}

func Table(err error) table.Client {
	return tableError{err}
}

func (err tableError) CreateSession(context.Context) (table.ClosableSession, error) {
	return nil, err
}

func (err tableError) Do(context.Context, table.Operation, ...table.Option) error {
	return err
}

func (err tableError) DoTx(context.Context, table.TxOperation, ...table.Option) error {
	return err
}

func (err tableError) Close(context.Context) error {
	return err
}

type schemeError struct {
	error
}

func Scheme(err error) scheme.Client {
	return schemeError{err}
}

func (err schemeError) Close(context.Context) error {
	return err
}

func (err schemeError) DescribePath(context.Context, string) (e scheme.Entry, _ error) {
	return e, err
}

func (err schemeError) MakeDirectory(context.Context, string) error {
	return err
}

func (err schemeError) ListDirectory(context.Context, string) (d scheme.Directory, _ error) {
	return d, err
}

func (err schemeError) RemoveDirectory(context.Context, string) error {
	return err
}

func (err schemeError) ModifyPermissions(context.Context, string, ...scheme.PermissionsOption) error {
	return err
}

type coordinationError struct {
	error
}

func Coordination(err error) coordination.Client {
	return coordinationError{err}
}

func (err coordinationError) Close(context.Context) error {
	return err
}

func (err coordinationError) CreateNode(context.Context, string, coordination.Config) error {
	return err
}

func (err coordinationError) AlterNode(context.Context, string, coordination.Config) error {
	return err
}

func (err coordinationError) DropNode(context.Context, string) error {
	return err
}

func (err coordinationError) DescribeNode(context.Context, string) (*scheme.Entry, *coordination.Config, error) {
	return nil, nil, err
}

type discoveryError struct {
	error
}

func Discovery(err error) discovery.Client {
	return discoveryError{err}
}

func (err discoveryError) Close(context.Context) error {
	return err
}

func (err discoveryError) Discover(context.Context) ([]endpoint.Endpoint, error) {
	return nil, err
}

func (err discoveryError) WhoAmI(context.Context) (*discovery.WhoAmI, error) {
	return nil, err
}

type ratelimiterError struct {
	error
}

func (err ratelimiterError) Close(context.Context) error {
	return err
}

func (err ratelimiterError) DescribeResource(context.Context, string, string) (*ratelimiter.Resource, error) {
	return nil, err
}

func (err ratelimiterError) AcquireResource(context.Context, string, string, uint64, bool) error {
	return err
}

func Ratelimiter(err error) ratelimiter.Client {
	return ratelimiterError{err}
}

func (err ratelimiterError) CreateResource(context.Context, string, ratelimiter.Resource) error {
	return err
}

func (err ratelimiterError) AlterResource(context.Context, string, ratelimiter.Resource) error {
	return err
}

func (err ratelimiterError) DropResource(context.Context, string, string) error {
	return err
}

func (err ratelimiterError) ListResource(context.Context, string, string, bool) ([]string, error) {
	return nil, err
}
