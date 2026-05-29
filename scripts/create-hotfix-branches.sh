#!/usr/bin/env bash
# Create local hotfix branches from v3.127.7 with partial reverts of bb5e8093 (#2040).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASE="v3.127.7"

set_version() {
	local suffix="$1"
	cat > internal/version/version.go <<EOF
package version

const (
	Major = "3"
	Minor = "127"
	Patch = "7"

	Package = "ydb-go-sdk"
)

const (
	Version     = Major + "." + Minor + "." + Patch + "-${suffix}"
	FullVersion = Package + "/" + Version
)
EOF
}

commit_hotfix() {
	local branch="$1"
	local suffix="$2"
	local msg="$3"
	git checkout -B "$branch" "$BASE"
	"$4"
	set_version "$suffix"
	git add -A
	git commit -m "$(cat <<EOF
${msg}

Partial revert of bb5e8093 (#2040) for preprod e2e bisect.
Base: ${BASE}, hotfix tag: ${suffix}.
EOF
)"
	echo "Created ${branch} ($(git rev-parse --short HEAD))"
}

# --- shared snippets ---

revert_stream_wrapper() {
	# Remove streamWrapper; restore v3.127.6 NewStream.
	python3 <<'PY'
from pathlib import Path
p = Path("internal/balancer/balancer.go")
text = p.read_text()
start = text.find("// streamWrapper wraps")
end = text.find("type Balancer struct {")
if start == -1 or end == -1:
    raise SystemExit("streamWrapper block not found")
text = text[:start] + text[end:]
old = '''	var stream grpc.ClientStream
	if err := b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		inner, innerErr := cc.NewStream(ctx, desc, method, opts...)
		if innerErr != nil {
			return innerErr
		}
		stream = &streamWrapper{
			ClientStream: inner,
			onErr: func(err error) {
				if IsBadConn(ctx, err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
					b.pool.Ban(ctx, cc, err)
				}
			},
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return stream, nil'''
new = '''	var client grpc.ClientStream
	err = b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		client, err = cc.NewStream(ctx, desc, method, opts...)

		return err
	})
	if err == nil {
		return client, nil
	}

	return nil, err'''
if old not in text:
    raise SystemExit("NewStream block not found")
text = text.replace(old, new)
# drop unused imports errors, io if present
text = text.replace('\t"errors"\n', '')
text = text.replace('\t"io"\n', '')
p.write_text(text)
PY
}

revert_pool_ban_filter() {
	python3 <<'PY'
from pathlib import Path
p = Path("internal/conn/pool.go")
text = p.read_text()
if "grpcCodes" not in text:
    text = text.replace(
        '"google.golang.org/grpc"\n',
        '"google.golang.org/grpc"\n\tgrpcCodes "google.golang.org/grpc/codes"\n',
        1,
    )
old = '''func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	if p.isClosed() {
		return
	}

	trace.DriverOnConnBan(
		p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Ban"),
		cc.Endpoint().Copy(), cc.GetState(), cause,
	)(cc.SetState(ctx, state.Banned))
}'''
new = '''func (p *Pool) Ban(ctx context.Context, cc Conn, cause error) {
	if p.isClosed() {
		return
	}

	if !xerrors.IsTransportError(cause,
		grpcCodes.ResourceExhausted,
		grpcCodes.Unavailable,
	) {
		return
	}

	e := cc.Endpoint().Copy()

	cc, ok := p.conns.Get(e.Key())
	if !ok {
		return
	}

	trace.DriverOnConnBan(
		p.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*Pool).Ban"),
		e, cc.GetState(), cause,
	)(cc.SetState(ctx, state.Banned))
}'''
if old not in text:
    raise SystemExit("Pool.Ban block not found")
p.write_text(text.replace(old, new))
PY
}

revert_overloaded_create_session() {
	sed -i '' 's/balancer\.BanOnOperationError(ctx, Ydb\.StatusIds_OVERLOADED),$/ctx,/' internal/query/session_core.go
	sed -i '' '/github.com\/ydb-platform\/ydb-go-sdk\/v3\/internal\/balancer/d' internal/query/session_core.go
	sed -i '' 's/balancer\.BanOnOperationError(ctx, Ydb\.StatusIds_OVERLOADED),$/ctx,/' internal/table/session.go
	sed -i '' '/github.com\/ydb-platform\/ydb-go-sdk\/v3\/internal\/balancer/d' internal/table/session.go
	# fix CreateSession call formatting
	python3 <<'PY'
from pathlib import Path
for rel in ("internal/query/session_core.go", "internal/table/session.go"):
    t = Path(rel).read_text()
    t = t.replace("client.CreateSession(\n\t\tctx,\n\t\t&", "client.CreateSession(ctx, &")
    t = t.replace("CreateSession(\n\t\tctx,\n\t\t&", "CreateSession(ctx, &")
    Path(rel).write_text(t)
PY
}

revert_wrapcall_conn_isbadconn() {
	git show v3.127.6:internal/conn/errors.go > internal/conn/errors.go
	sed -i '' 's/IsBadConn(ctx, err, b.driverConfig/conn.IsBadConn(err, b.driverConfig/' internal/balancer/balancer.go
}

revert_stream_ban_v1276() {
	revert_stream_wrapper
	revert_pool_ban_filter
	git show v3.127.6:internal/conn/conn.go > /tmp/conn_v1276.go
	python3 <<'PY'
from pathlib import Path
import re

cur = Path("internal/conn/conn.go").read_text()
old = Path("/tmp/conn_v1276.go").read_text()

def extract(name, src):
    m = re.search(rf'func \(c \*conn\) {name}\([^)]*\)[^{{]*\{{', src)
    if not m:
        return None
    i = m.start()
    depth = 0
    for j in range(m.end()-1, len(src)):
        if src[j] == '{':
            depth += 1
        elif src[j] == '}':
            depth -= 1
            if depth == 0:
                return src[i:j+1]
    return None

# restore onTransportErrors field
cur = cur.replace(
    "\t\tonClose      []func(*conn)\n\t}",
    "\t\tonClose           []func(*conn)\n\t\tonTransportErrors []func(ctx context.Context, cc Conn, cause error)\n\t}",
)

on_transport = extract("onTransportError", old)
with_opt = re.search(r'func withOnTransportError[\s\S]*?^}\n', old, re.M)
if not on_transport or not with_opt:
    raise SystemExit("onTransportError helpers missing in v3.127.6")

if "onTransportError(ctx context.Context, cause error)" not in cur:
    insert_at = cur.find("func (c *conn) GetState()")
    cur = cur[:insert_at] + on_transport + "\n\n" + cur[insert_at:]

if "withOnTransportError" not in cur:
    insert_at = cur.find("func newConn(")
    cur = cur[:insert_at] + with_opt.group(0) + "\n" + cur[insert_at:]

Path("internal/conn/conn.go").write_text(cur)
PY
	# pool: register ban callback on new conns
	python3 <<'PY'
from pathlib import Path
p = Path("internal/conn/pool.go")
t = p.read_text()
t = t.replace(
    "cc = newConn(endpoint, p,\n\t\twithOnClose(p.remove),\n\t)",
    "cc = newConn(endpoint, p,\n\t\twithOnClose(p.remove),\n\t\twithOnTransportError(p.Ban),\n\t)",
)
p.write_text(t)
PY
	# grpc stream: restore onTransportError defer on Send/Recv
	git show v3.127.6:internal/conn/grpc_client_stream.go > /tmp/grpc_stream_v1276.go
	python3 <<'PY'
from pathlib import Path
import re
cur = Path("internal/conn/grpc_client_stream.go").read_text()
old = Path("/tmp/grpc_stream_v1276.go").read_text()
for fn in ("SendMsg", "RecvMsg"):
    m_old = re.search(rf'func \(s \*grpcClientStream\) {fn}\([\s\S]*?^\treturn', old, re.M)
    m_cur = re.search(rf'func \(s \*grpcClientStream\) {fn}\([\s\S]*?^\treturn', cur, re.M)
    if not m_old or not m_cur:
        raise SystemExit(f"{fn} block missing")
    if "onTransportError" in m_cur.group(0):
        continue
    cur = cur.replace(m_cur.group(0), m_old.group(0))
Path("internal/conn/grpc_client_stream.go").write_text(cur)
PY
}

full_revert_bb5e8093() {
	git checkout v3.127.6 -- \
		internal/balancer/balancer.go \
		internal/balancer/connections_state.go \
		internal/balancer/connections_state_test.go \
		internal/conn/conn.go \
		internal/conn/conn_test.go \
		internal/conn/errors.go \
		internal/conn/errors_test.go \
		internal/conn/grpc_client_stream.go \
		internal/conn/grpc_client_stream_test.go \
		internal/conn/middleware_test.go \
		internal/conn/pool.go \
		internal/conn/pool_test.go \
		internal/conn/state.go \
		internal/conn/state_test.go \
		internal/query/session_core.go \
		internal/table/session.go \
		balancers/balancers_test.go \
		internal/mock/conn.go \
		internal/mock/grpc_client_conn_interface_mock.go \
		internal/mock/grpc_client_stream_mock.go
	rm -f internal/balancer/errors.go internal/balancer/errors_test.go internal/balancer/balancer_test.go
	# state package move: v3.127.6 uses conn/state.go not conn/state/state.go
	rm -rf internal/conn/state
	mkdir -p internal/conn/state
	git show v3.127.6:internal/conn/state.go > internal/conn/state/state.go
	git show v3.127.6:internal/conn/state_test.go > internal/conn/state/state_test.go
	# fix imports conn.State -> state.State in checked-out files if needed
	# v3.127.6 balancer uses conn.Banned - need to fix after checkout
	python3 <<'PY'
from pathlib import Path
import subprocess, re
files = subprocess.check_output(["git", "diff", "--name-only"], text=True).splitlines()
for f in files:
    p = Path(f)
    if not p.is_file() or p.suffix != ".go":
        continue
    t = p.read_text()
    nt = t.replace("github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state", "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state")
    # v3.127.6 files use conn.Banned inline in pool - already correct from checkout
    p.write_text(nt)
PY
}

commit_hotfix "hotfix/revert-stream-wrapper" "hotfix.1" \
	"hotfix: remove streamWrapper from balancer.NewStream (#2040 partial revert)" \
	"revert_stream_wrapper"

commit_hotfix "hotfix/revert-pool-ban-filter" "hotfix.2" \
	"hotfix: restore pool.Ban filter (ResourceExhausted/Unavailable only)" \
	"revert_pool_ban_filter"

commit_hotfix "hotfix/revert-overloaded-create-session" "hotfix.3" \
	"hotfix: remove BanOnOperationError(OVERLOADED) on CreateSession" \
	"revert_overloaded_create_session"

commit_hotfix "hotfix/revert-stream-wrapper-and-pool-ban-filter" "hotfix.4" \
	"hotfix: no streamWrapper + restricted pool.Ban (combo)" \
	"revert_stream_wrapper; revert_pool_ban_filter"

commit_hotfix "hotfix/revert-wrapcall-conn-isbadconn" "hotfix.5" \
	"hotfix: use conn.IsBadConn in wrapCall instead of balancer.IsBadConn" \
	"revert_wrapcall_conn_isbadconn"

commit_hotfix "hotfix/revert-stream-ban-v1276" "hotfix.6" \
	"hotfix: restore v3.127.6 stream ban path (onTransportError + no streamWrapper + pool filter)" \
	"revert_stream_ban_v1276"

commit_hotfix "hotfix/revert-bb5e8093-full" "hotfix.7" \
	"hotfix: full revert of bb5e8093 (#2040), keep v3.127.7 grpc bump" \
	"full_revert_bb5e8093"

echo "Done. Branches:"
git branch --list 'hotfix/*'
