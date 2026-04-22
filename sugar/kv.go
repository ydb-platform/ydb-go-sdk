package sugar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// kvClientBuilder is a Redis-like string key-value store backed by YDB.
type (
	columnModel struct {
		Key, Value, Expire string
	}
	tableConfig struct {
		tablePath   string
		api         api
		createTable bool
		cols        columnModel
	}
	kvConfig struct {
		tableConfig

		api api
	}
	kvClientBuilder struct {
		ctx    context.Context
		config kvConfig
		db     *ydb.Driver
	}
	kvClient struct {
		config kvConfig
		db     *ydb.Driver
	}
)

// ClientBackend selects how GET/SET are executed. KEYS always uses [ydb.Driver.Query].
type api int

func (api api) String() any {
	switch api {
	case KV_API_QUERY:
		return "QUERY"
	case KV_API_KEY_VALUE:
		return "KV"
	default:
		return fmt.Sprintf("unknown API: %d", api)
	}
}

const (
	// KV_API_QUERY uses YQL via [ydb.Driver.Query] for all data paths including DEL and KEYS.
	KV_API_QUERY api = iota
	// KV_API_KEY_VALUE uses [ydb.Driver.Table] BulkUpsert for SET and ReadRows for GET.
	// DEL and KEYS still use Query.
	KV_API_KEY_VALUE

	defaultAPI          = KV_API_KEY_VALUE
	defaultTablePath    = "kv"
	defaultKeyColumn    = "key"
	defaultValueColumn  = "value"
	defaultExpireColumn = "expire_at"
)

var (
	nullTimestamp = types.NullValue(types.TypeTimestamp)
)

func quote(name string) string {
	return "`" + name + "`"
}

func NewKV(ctx context.Context, db *ydb.Driver) kvClientBuilder {
	return kvClientBuilder{
		ctx: ctx,
		config: kvConfig{
			tableConfig: tableConfig{
				tablePath:   defaultTablePath,
				createTable: true,
				cols: columnModel{
					Key:    defaultKeyColumn,
					Value:  defaultValueColumn,
					Expire: defaultExpireColumn,
				},
			},
			api: defaultAPI,
		},
		db: db,
	}
}

func (builder kvClientBuilder) WithAPI(api api) kvClientBuilder {
	builder.config.api = api

	return builder
}

func (builder kvClientBuilder) WithTable(tablePath string) kvClientBuilder {
	builder.config.tablePath = tablePath

	return builder
}

func (builder kvClientBuilder) WithCreateTableIfNotExists(b bool) kvClientBuilder {
	builder.config.createTable = b

	return builder
}

func (builder kvClientBuilder) WithColumnNameForKey(name string) kvClientBuilder {
	if name != "" {
		builder.config.cols.Key = name
	}

	return builder
}

func (builder kvClientBuilder) WithColumnNameForValue(name string) kvClientBuilder {
	if name != "" {
		builder.config.cols.Value = name
	}

	return builder
}

func (builder kvClientBuilder) WithColumnNameForExpire(name string) kvClientBuilder {
	if name != "" {
		builder.config.cols.Expire = name
	}

	return builder
}

func (builder kvClientBuilder) Build() (*kvClient, error) {
	client := &kvClient{
		config: builder.config,
		db:     builder.db,
	}

	client.config.tablePath = strings.Join([]string{client.db.Name(), client.config.tablePath}, "/")

	if client.config.createTable {
		if err := client.db.Query().Exec(builder.ctx,
			fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					%s Text NOT NULL,
					%s Bytes NOT NULL,
					%s Timestamp,
					PRIMARY KEY (%s)
				) WITH (
					TTL = Interval("PT1H") ON %s,
					STORE = ROW,
					AUTO_PARTITIONING_BY_SIZE = ENABLED,
					AUTO_PARTITIONING_BY_LOAD = ENABLED,
					AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
					AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
				);`,
				quote(client.config.tablePath),
				quote(client.config.cols.Key),
				quote(client.config.cols.Value),
				quote(client.config.cols.Expire),
				quote(client.config.cols.Key),
				quote(client.config.cols.Expire),
			), query.WithIdempotent(),
		); err != nil {
			return nil, xerrors.WithStackTrace(fmt.Errorf("create table failed: %w", err))
		}
	} else {
		if err := client.verifyTable(builder.ctx, client.config.tablePath); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
	}

	return client, nil
}

func (c *kvClient) verifyTable(ctx context.Context, absPath string) error {
	e, err := c.db.Scheme().DescribePath(ctx, absPath)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("describe %q failed: %w", absPath, err))
	}

	if !(e.IsTable() || e.IsColumnTable()) {
		return xerrors.WithStackTrace(fmt.Errorf("%q is not a table (type=%s)", absPath, e.Type.String()))
	}

	return nil
}

func (c *kvClient) keyColumn() string    { return quote(c.config.cols.Key) }
func (c *kvClient) valueColumn() string  { return quote(c.config.cols.Value) }
func (c *kvClient) expireColumn() string { return quote(c.config.cols.Expire) }

// API reports how GET/SET are executed.
func (c *kvClient) API() api { return c.config.api }

// Close is a no-op; TTL-based eviction is handled by the YDB table TTL setting.
func (*kvClient) Close() {}

// Get returns the value for key. [ErrNotFound] if missing or expired.
func (c *kvClient) Get(ctx context.Context, key string) ([]byte, error) {
	if c.config.api == KV_API_KEY_VALUE {
		vv, err := c.getValueByKeyUsingReadRows(ctx, key)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return vv, nil
	}

	row, err := c.db.Query().QueryRow(ctx,
		fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s = $key AND (%s IS NULL OR %s > CurrentUtcTimestamp());
		`, c.valueColumn(), quote(c.config.tablePath), c.keyColumn(), c.expireColumn(), c.expireColumn()),
		query.WithParameters(ydb.ParamsBuilder().Param("$key").Text(key).Build()),
		query.WithIdempotent(),
	)
	if err != nil {
		if errors.Is(err, query.ErrNoRows) {
			return nil, xerrors.WithStackTrace(io.EOF)
		}

		return nil, xerrors.WithStackTrace(fmt.Errorf("redis get: %w", err))
	}

	var v []byte
	if err := row.ScanNamed(query.Named(c.config.cols.Value, &v)); err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("redis get scan: %w", err))
	}

	return v, nil
}

func (c *kvClient) getValueByKeyUsingReadRows(ctx context.Context, key string) ([]byte, error) {
	keyStruct := types.StructValue(
		types.StructFieldValue(c.config.cols.Key, types.TextValue(key)),
	)
	res, err := c.db.Table().ReadRows(ctx, c.config.tablePath,
		types.ListValue(keyStruct),
		[]options.ReadRowsOption{
			options.ReadColumn(c.config.cols.Value),
			options.ReadColumn(c.config.cols.Expire),
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("redis get readrows: %w", err))
	}

	defer func() { _ = res.Close() }()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		if err := res.Err(); err != nil {
			return nil, xerrors.WithStackTrace(fmt.Errorf("redis get result: %w", err))
		}

		return nil, xerrors.WithStackTrace(io.EOF)
	}

	var (
		v   []byte
		exp *time.Time
	)

	if err := res.ScanNamed(
		named.OptionalWithDefault(c.config.cols.Value, &v),
		named.Optional(c.config.cols.Expire, &exp),
	); err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("redis get scan: %w", err))
	}

	if exp != nil && !exp.After(time.Now().UTC()) {
		return nil, xerrors.WithStackTrace(io.EOF)
	}

	return v, nil
}

// Set stores value under key. Use [withTTL] or [WithExpireAt] for expiration.
func (c *kvClient) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	expireAt := nullTimestamp
	if ttl != nil {
		switch {
		case *ttl > 0:
			expireAt = types.OptionalValue(types.TimestampValueFromTime(time.Now().UTC().Add(*ttl)))
		case *ttl == 0:
			// nop
		default:
			return xerrors.WithStackTrace(fmt.Errorf("redis: ttl must be positive"))
		}
	}

	if c.config.api == KV_API_KEY_VALUE {
		err := c.db.Table().BulkUpsert(ctx,
			c.config.tablePath,
			table.BulkUpsertDataRows(types.ListValue(types.StructValue(
				types.StructFieldValue(c.config.cols.Key, types.TextValue(key)),
				types.StructFieldValue(c.config.cols.Value, types.BytesValue(value)),
				types.StructFieldValue(c.config.cols.Expire, expireAt),
			))),
			table.WithIdempotent(),
		)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}

	err := c.db.Query().Exec(ctx,
		fmt.Sprintf(`
			UPSERT INTO %s (%s, %s, %s) VALUES ($key, $value, $expire_at)
		`, quote(c.config.tablePath), c.keyColumn(), c.valueColumn(), c.expireColumn()),
		query.WithParameters(ydb.ParamsBuilder().
			Param("$key").Text(key).
			Param("$value").Bytes(value).
			Param("$expire_at").Any(expireAt).
			Build(),
		),
		query.WithIdempotent(),
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// Del removes keys. Returns the number of keys that were present and not yet expired (Redis semantics).
func (c *kvClient) Del(ctx context.Context, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	lb := ydb.ParamsBuilder().Param("$keys").BeginList()
	for _, k := range keys {
		lb = lb.Add().Text(k)
	}
	params := lb.EndList().Build()

	q := fmt.Sprintf(`
		DELETE FROM %s
		WHERE %s IN $keys
		RETURNING %s, %s;
	`, c.config.tablePath, c.keyColumn(), c.keyColumn(), c.expireColumn())
	rs, err := c.db.Query().QueryResultSet(ctx, q,
		query.WithParameters(params),
		query.WithIdempotent(),
	)
	if err != nil {
		return 0, xerrors.WithStackTrace(fmt.Errorf("redis del: %w", err))
	}

	defer func() { _ = rs.Close(ctx) }()

	now := time.Now().UTC()
	n := 0
	for row, err := range rs.Rows(ctx) {
		if err != nil {
			return n, xerrors.WithStackTrace(fmt.Errorf("redis del row: %w", err))
		}

		var key string
		var exp *time.Time
		if err := row.Scan(&key, &exp); err != nil {
			return n, xerrors.WithStackTrace(fmt.Errorf("redis del scan: %w", err))
		}

		if exp == nil || exp.After(now) {
			n++
		}
	}

	return n, nil
}

// Keys returns keys matching pattern (Redis KEYS). Always uses YQL via [ydb.Driver.Query].
func (c *kvClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	keyCol := c.keyColumn()

	lp, err := redisKeysPatternToYQL(pattern)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("redis keys pattern: %w", err))
	}

	var (
		q      string
		params ydb.Params
	)
	if lp.useMatch {
		q = fmt.Sprintf(`
			SELECT %s AS rkey, %s AS expire_at FROM %s
			WHERE (%s IS NULL OR %s > CurrentUtcTimestamp())
			  AND %s MATCH $re;
		`, keyCol, c.expireColumn(), c.config.tablePath, c.expireColumn(), c.expireColumn(), keyCol)
		params = ydb.ParamsBuilder().Param("$re").Text(lp.re2).Build()
	} else {
		q = fmt.Sprintf(`
			SELECT %s AS rkey, %s AS expire_at FROM %s
			WHERE (%s IS NULL OR %s > CurrentUtcTimestamp())
			  AND %s LIKE $like ESCAPE '!';
		`, keyCol, c.expireColumn(), c.config.tablePath, c.expireColumn(), c.expireColumn(), keyCol)
		params = ydb.ParamsBuilder().Param("$like").Text(lp.like).Build()
	}
	rs, err := c.db.Query().QueryResultSet(ctx, q,
		query.WithParameters(params),
		query.WithIdempotent(),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("redis keys: %w", err))
	}

	defer func() { _ = rs.Close(ctx) }()

	var out []string
	for row, err := range rs.Rows(ctx) {
		if err != nil {
			return nil, xerrors.WithStackTrace(fmt.Errorf("redis keys row: %w", err))
		}

		var k string
		var exp *time.Time
		if err := row.ScanNamed(
			query.Named("rkey", &k),
			query.Named("expire_at", &exp),
		); err != nil {
			return nil, xerrors.WithStackTrace(fmt.Errorf("redis keys scan: %w", err))
		}
		if exp != nil && !exp.After(time.Now().UTC()) {
			continue
		}
		out = append(out, k)
	}

	return out, nil
}

// listPattern describes how to filter keys in [kvClientBuilder.List].
type listPattern struct {
	// useMatch means use Re2::Match (YQL MATCH); otherwise LIKE with ESCAPE.
	useMatch bool
	like     string
	re2      string
}

// redisKeysPatternToYQL converts a Redis KEYS-style glob to either a LIKE pattern (ESCAPE '!')
// or a full-string Re2 pattern for MATCH when the glob contains character classes `[` `]`.
func redisKeysPatternToYQL(pattern string) (listPattern, error) {
	if pattern == "" {
		pattern = "*"
	}
	if strings.ContainsAny(pattern, "[]") {
		re, err := redisGlobToRE2Match(pattern)
		if err != nil {
			return listPattern{}, xerrors.WithStackTrace(err)
		}

		return listPattern{useMatch: true, re2: re}, nil
	}

	return listPattern{useMatch: false, like: redisGlobToLIKE(pattern)}, nil
}

// redisGlobToLIKE maps Redis `*` and `?` to SQL `%` and `_`, and escapes `%`, `_`, and `!`
// for use with LIKE ... ESCAPE '!'. Backslash escapes the next character (Redis semantics).
func redisGlobToLIKE(glob string) string {
	var b strings.Builder
	escape := byte('!')
	i := 0
	for i < len(glob) {
		if glob[i] == '\\' && i+1 < len(glob) {
			i++
			writeLIKELiteral(&b, escape, glob[i])
			i++
			continue
		}
		switch glob[i] {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_':
			b.WriteByte(escape)
			b.WriteByte(glob[i])
		case '!':
			b.WriteByte(escape)
			b.WriteByte('!')
		default:
			b.WriteByte(glob[i])
		}
		i++
	}

	return b.String()
}

func writeLIKELiteral(b *strings.Builder, esc byte, c byte) {
	switch c {
	case '%', '_':
		b.WriteByte(esc)
		b.WriteByte(c)
	case '!':
		b.WriteByte(esc)
		b.WriteByte('!')
	default:
		b.WriteByte(c)
	}
}

// redisGlobToRE2Match builds a Re2 pattern for YQL MATCH (full string match).
// Supports `*`, `?`, `[...]`, `[!...]`, and `\` escapes.
func redisGlobToRE2Match(glob string) (string, error) {
	var b strings.Builder
	b.WriteByte('^')
	i := 0
	for i < len(glob) {
		if glob[i] == '\\' && i+1 < len(glob) {
			b.WriteString(regexp.QuoteMeta(string(glob[i+1])))
			i += 2
			continue
		}
		switch glob[i] {
		case '*':
			b.WriteString(".*")
			i++
		case '?':
			b.WriteByte('.')
			i++
		case '[':
			end := strings.IndexByte(glob[i+1:], ']')
			if end < 0 {
				return "", xerrors.WithStackTrace(fmt.Errorf("unclosed '[' in KEYS pattern"))
			}

			end += i + 1
			inner := glob[i+1 : end]
			if inner == "" {
				return "", xerrors.WithStackTrace(fmt.Errorf("empty character class in KEYS pattern"))
			}

			if inner[0] == '!' {
				b.WriteString("[^")
				b.WriteString(inner[1:])
				b.WriteByte(']')
			} else {
				b.WriteByte('[')
				b.WriteString(inner)
				b.WriteByte(']')
			}
			i = end + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(glob[i])))
			i++
		}
	}
	b.WriteByte('$')

	return b.String(), nil
}
