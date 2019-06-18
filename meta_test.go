package ydb

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestMetaErrDropToken(t *testing.T) {
	var call int
	m := newMeta("database", CredentialsFunc(func(context.Context) (string, error) {
		if call == 0 {
			call++
			return "token", nil
		}
		return "", ErrCredentialsDropToken
	}))

	md1, err := m.md(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertMetaHasDatabase(t, md1)
	assertMetaHasToken(t, md1)

	md2, err := m.md(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertMetaHasDatabase(t, md2)
	assertMetaHasNoToken(t, md2)
}

func TestMetaErrKeepToken(t *testing.T) {
	var call int
	m := newMeta("database", CredentialsFunc(func(context.Context) (string, error) {
		if call == 0 {
			call++
			return "token", nil
		}
		return "", ErrCredentialsKeepToken
	}))

	md1, err := m.md(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertMetaHasDatabase(t, md1)
	assertMetaHasToken(t, md1)

	md2, err := m.md(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertMetaHasDatabase(t, md2)
	assertMetaHasToken(t, md1)
}

func assertMetaHasDatabase(t *testing.T, md metadata.MD) {
	if len(md.Get(metaDatabase)) == 0 {
		t.Errorf("no database info in meta")
	}
}
func assertMetaHasToken(t *testing.T, md metadata.MD) {
	if len(md.Get(metaTicket)) == 0 {
		t.Errorf("no token info in meta")
	}
}
func assertMetaHasNoToken(t *testing.T, md metadata.MD) {
	if len(md.Get(metaTicket)) != 0 {
		t.Errorf("unexpected token info in meta")
	}
}
