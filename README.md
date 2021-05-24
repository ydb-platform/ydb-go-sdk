# ydb

> YDB API client written in Go.

[godoc](https://godoc.org/github.com/yandex-cloud/ydb-go-sdk/)

## Overview

Currently package ydb provides **scheme** and **table** API client implementations for YDB.

## Prerequisites

Requires Go 1.13 or later.

## Install

```
go get -u github.com/yandex-cloud/ydb-go-sdk
```

## Usage

The straightforward example of querying data may looks similar to this:

```go
	dialer := &ydb.Dialer{
        DriverConfig: &ydb.DriverConfig{
            Database: "/ru/home/username/db",
            Credentials: ydb.AuthTokenCredentials{
                AuthToken: os.Getenv("YDB_TOKEN"),
            },
        },
        TLSConfig:    &tls.Config{/*...*/},
        Timeout:      time.Second,
    }
    driver, err := dialer.Dial(ctx, "ydb-ru.yandex.net:2135")
	if err != nil {
		// handle error
	}
	tc := table.Client{
		Driver: driver,
	}
	s, err := tc.CreateSession(ctx)
	if err != nil {
		// handle error
	}
	defer s.Close(ctx)

	// Prepare transaction control for upcoming query execution.
	// NOTE: result of TxControl() may be reused.
	txc := table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)

	// Execute text query without preparation and with given "autocommit"
	// transaction control. That is, transaction will be commited without
	// additional calls. Notice the "_" unused variable – it stands for created
	// transaction during execution, but as said above, transaction is commited
	// for us and we do not want to do anything with it.
	_, res, err := s.Execute(ctx, txc,
		`--!syntax_v1
DECLARE $mystr AS Utf8?; SELECT 42 as id, $mystr as mystr`,
		table.NewQueryParameters(
			table.ValueParam("$mystr", ydb.OptionalValue(ydb.UTF8Value("test"))),
		),
	)
	if err != nil {
		return err // handle error
	}
	// Scan for received values within the result set(s).
	// Note that Next*() methods report about success of advancing, while
	// res.Err() reports the reason of last unsuccessful one.
	for res.NextSet() {
		for res.NextRow() {
			// Suppose our "users" table has two rows: id and age.
			// Thus, current row will contain two appropriate items with
			// exactly the same order.
			//
			// Note the O*() getters. "O" stands for Optional. That is,
			// currently, all columns in tables are optional types.
			res.NextItem()
			id := res.Int32()

			res.NextItem()
			myStr := res.OUTF8()

			// Note that any value getter (such that OUTF8() and Int32()
			// above) may fail the result scanning. When this happens, getter
			// function returns zero value of requested type and marks result
			// scanning as failed, preventing any further scanning. In this
			// case res.Err() will return the cause of fail.
			if res.Err() == nil {
				// do something with data
				fmt.Printf("got id %v, got mystr: %v\n", id, myStr)
			} else {
				return res.Err() // handle error
			}
		}
	}
	if res.Err() != nil {
		return res.Err() // handle error
	}
```

This example can be tested as `ydb/example/from_readme`

YDB sessions may become staled and appropriate error will be returned. To
reduce boilerplate overhead for such cases `ydb` provides generic retry logic:

```go
	// Prepare session pool to be used during retries.
	sp := table.SessionPool{
		SizeLimit:          -1,          // No limits for pool size.
		KeepAliveBatchSize: -1,          // Keep alive as much as possible number of sessions.
		IdleThreshold:      time.Second, // Keep alive idle session every second.
		Builder:            &tc,         // Create new sessions within tc.
	}
	defer sp.Reset(ctx) // Close all sessions within pool.

	var res *table.Result
	// Retry() will call given OperationFunc with the following invariants:
	//  - previous operation failed with retriable error;
	//  - number of retries is under the limit (default to 10, see table.Retryer docs);
	//
	// Note that in case of prepared statements call to Prepare() must be made
	// inside the Operation body.
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.Execute(...)
			return
		}),
	)
```

That is, instead of manual creation of `table.Session`, we give a
`SessionPool` such responsibility. It holds instances of active sessions and
"pings" them periodically to keep them alive.

See `table.Retryer` docs for more information about retry options.

### database/sql driver

There is a `database/sql` driver for the sql-based applications or those which
by some reasons need to use additional layer of absctraction between user code
and storage backend.

For more information please see the docs of `ydb/ydbsql` package which provides
`database/sql` driver implementation.

## Credentials

There are different variants to get `ydb.Credentials` object to get authorized.
Usage examples can be found [here](example/internal/cli/driver.go) at `func credentials(...) ydb.Credentials`.

## Generating code

### Overview

There is lot of boilerplate code for scanning values from query result and for
passing values to prepare query. There is an **experimental** tool named
`ydbgen` aimed to solve this.

Currently it is possible to generate such things:
- scanning values from result into a struct or slice of structs;
- building query parameters from struct
- building ydb's struct value from struct
- building ydb's list value from slice of structs

The very short example could be like this:

```go
package somepkg

//go:generate ydbgen

//ydb:gen scan
type User struct {
	Name string
	Age  int32
}
```

After running `go generate path/to/somepkg/dir` file with suffix `_ydbgen.go`
will be generated. It will contain method `Scan()` for `User` type, as
requested in the *generate comment*.

### Configuration

Generation may be configured at three levels starting from top:
- ydbgen binary flags (package level)
- comment markers right before generation object in form of `//ydb:set
  [key1:value1 [... keyN:valueN]]` (type level)
- [struct tags](https://golang.org/ref/spec#Tag) (field level)

Each downstream level overrides options for its context.

For example, this code will generate all possible code for `User` struct with
field `Age` type mapped to **non-optional** type, because the lowermost
configuration level (which is struct tag) defines non-optional `uint32` type:

```go
//go:generate ydbgen -wrap optional

//ydb:gen
//ydb:set wrap:none
type User struct {
	Age int32 `ydb:"type:uint32,column:user_age"`
}
```


#### Binary flags

 Flag   | Value      | Default | Meaning
--------|:----------:|:-------:|---------
 `wrap` | `optional` | +       | Wraps all mapped field types with optional type if no explicit tag is specified.
 `wrap` | `none`     |         | No wrapping performed.
 `seek` | `column`   | +       | Uses res.SeekItem() call to find out next field to scan.
 `seek` | `position` |         | Uses res.NextItem() call to find out next field to scan.

#### Comment markers options

Options for comment markers are similar to flags, except the form of
serialization.

#### Struct tags and values overview

 Tag      | Value    | Default | Meaning
----------|:--------:|:-------:|---------
 `type`   | `T`      |         | Specifies which ydb primitive type must be used for this field.
 `type`   | `T?`     |         | The same as above, but wraps T with optional type.
 `conv`   | `safe`   | +       | Prepares only safe type conversions. Fail generation if conversion is not possible.
 `conv`   | `unsafe` |         | Prepares unsafe type conversions too.
 `conv`   | `assert` |         | Prepares safety assertions before type conversion.
 `column` | `string` |         | Maps field to this column name.

Also the shorthand tags are possible: when using tag without `key:value` form,
tag with `-` value is interpreted as field must be ignored; in other way it is
interpreted as the column name.

### Customization

There are few additional options existing for flexibility purposes.

#### Optional Types

Previously only basic Go types were mentioned as ones that able to be converted
to ydb types. But it is possible generate code that maps defined type to YDB
type (actually to basic Go type and then to YDB type). To make so, such type
must provide two methods (when generation both getter and setters) – `Get() (T,
bool)` and `Set(T)`, where `T` is a basic Go type, and `bool` is a flag that
indicates that value defined.

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Name OptString
}

type OptString struct {
	Value   string
	Defined bool
}

func (s OptString) Get() (string, bool) {
	return s.Value, s.Defined
}
func (s *OptString) Set(v string) {
	*s = OptString{
		Value:   v,
		Defined: true,
	}
}
```

There is special package called `ydb/opt` for this purposes:

```go
package main

import "github.com/yandex-cloud/ydb-go-sdk/opt"

//go:generate ydbgen

//ydb:gen
type User struct {
	Name opt.String
}
```

#### Dealing with time.Time

There is additional feature that makes it easier to work with `time.Time`
values and their conversion to YDB types:

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Updated time.Time `ydb:"type:timestamp?"`
}
```

#### Dealing with container types.

`ydbgen` supports scanning and serializing container types such as `List<T>` or `Struct<T>`.

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Tags []string `ydb:"type:list<string>"`
}
```

Example above will interpret value for `tags` column (or 0-th item, depending
on the `seek` mode) as `List<String>`.

Note that for `String` type this is neccessary to inform `ydbgen` that it is
not a container by setting `type` field tag.

> For more info please look at `ydb/examples/generation` folder.

## Examples

More examples are listed in `ydb/examples` directory.

