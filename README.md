# ydb

[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3)
[![GoDoc](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3?status.svg)](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3)
![tests](https://github.com/ydb-platform/ydb-go-sdk/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/ydb-platform/ydb-go-sdk/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/ydb-go-sdk)](https://goreportcard.com/report/github.com/ydb-platform/ydb-go-sdk)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-go-sdk/branch/master/graph/badge.svg)](https://app.codecov.io/gh/ydb-platform/ydb-go-sdk)

> YDB API client written in Go.

[godoc](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3/)

## Table of contents
1. [Overview](#Overview)
2. [About semantic versioning](#SemVer)
3. [Prerequisites](#Prerequisites)
4. [Installation](#Install)
5. [Usage](#Usage)
6. [Credentials](#Credentials)
7. [Examples](#examples)

## Overview <a name="Overview"></a>

Currently package ydb provides `scheme` and `table` API client implementations for `YDB`.

## About semantic versioning <a name="SemVer"></a>

We follow the **[SemVer 2.0.0](https://semver.org)**. In particular, we provide backward compatibility in the `MAJOR` releases. New features without loss of backward compatibility appear on the `MINOR` release. In the minor version, the patch number starts from `0`. Bug fixes and internal changes are released with the third digit (`PATCH`) in the version.

There are, however, some changes with the loss of backward compatibility that we consider to be `MINOR`:
* extension or modification of internal `ydb-go-sdk` interfaces. We understand that this will break the compatibility of custom implementations of the `ydb-go-sdk` internal interfaces. But we believe that the internal interfaces of `ydb-go-sdk` are implemented well enough that they do not require custom implementation. We are working to ensure that all internal interfaces have limited access only inside `ydb-go-sdk`.
* major changes to (including removal of) the public interfaces and types that have been previously exported by `ydb-go-sdk`. We understand that these changes will break the backward compatibility of early adopters of these interfaces. However, these changes are generally coordinated with early adopters and have the concise interfacing with `ydb-go-sdk` as a goal.

Internal interfaces outside from `internal` directory are marked with comment such as
```
// Warning: only for internal usage inside ydb-go-sdk
```

We publish the planned breaking `MAJOR` changes:
* via the comment `Deprecated` in the code indicating what should be used instead
* through the file [`NEXT_MAJOR_RELEASE.md`](#NEXT_MAJOR_RELEASE.md)

## Prerequisites <a name="Prerequisites"></a>

Requires Go 1.13 or later.

## Installation <a name="Installation"></a>

```
go get -u github.com/ydb-platform/ydb-go-sdk/v3
```

## Usage <a name="Usage"></a>

The straightforward example of querying data may look similar to this:

```go
   // Determine timeout for connect or do nothing
   ctx := context.Background()

   // connect package helps to connect to database, returns connection object which
   // provide necessary clients such as table.Client, scheme.Client, etc.
   db, err := ydb.New(
      ctx,
      connectParams,
	  ydb.WithConnectionString(ydb.ConnectionString(os.Getenv("YDB_CONNECTION_STRING"))),
      ydb.WithDialTimeout(3 * time.Second),
      ydb.WithCertificatesFromFile("~/.ydb/CA.pem"),
      ydb.WithSessionPoolIdleThreshold(time.Second * 5),
      ydb.WithSessionPoolKeepAliveMinSize(-1),
      ydb.WithDiscoveryInterval(5 * time.Second),
      ydb.WithAccessTokenCredentials(os.GetEnv("YDB_ACCESS_TOKEN_CREDENTIALS")),
   )
   if err != nil {
      // handle error
   }
   defer func() { _ = db.Close() }()
   
   // Create session for execute queries
   session, err := db.Table().CreateSession(ctx)
   if err != nil {
       // handle error
   }
   defer session.Close(ctx)

   // Prepare transaction control for upcoming query execution.
   // NOTE: result of TxControl() may be reused.
   txc := table.TxControl(
       table.BeginTx(table.WithSerializableReadWrite()),
       table.CommitTx(),
   )

   // Execute text query without preparation and with given "autocommit"
   // transaction control. That is, transaction will be committed without
   // additional calls. Notice the "_" unused variable – it stands for created
   // transaction during execution, but as said above, transaction is committed
   // for us and `ydb-go-sdk` do not want to do anything with it.
   _, res, err := session.Execute(ctx, txc,
       `--!syntax_v1
           DECLARE $mystr AS Utf8?;
           SELECT 42 as id, $mystr as mystr
       `,
       table.NewQueryParameters(
           table.ValueParam("$mystr", types.OptionalValue(types.UTF8Value("test"))),
       ),
   )
   if err != nil {
       return err // handle error
   }
   // Scan for received values within the result set(s).
   // res.Err() reports the reason of last unsuccessful one.
   var (
       id    int32
       myStr *string //optional value
   )
   for res.NextResultSet("id", "mystr") {
       for res.NextRow() {
           // Suppose our "users" table has two rows: id and age.
           // Thus, current row will contain two appropriate items with
           // exactly the same order.
           err := res.Scan(&id, &myStr)
   
           // Error handling.
           if err != nil {
               return err
           }
           // do something with data
           fmt.Printf("got id %v, got mystr: %v\n", id, *myStr)
       }
   }
   if res.Err() != nil {
       return res.Err() // handle error
   }
```

This example can be tested as https://github.com/ydb-platform/ydb-go-examples/tree/master/from_readme

YDB sessions may become staled and appropriate error will be returned. To
reduce boilerplate overhead for such cases `ydb-go-sdk` provides generic retry logic:

```go
 var res *table.Result
 // Retry() provide the best effort fo retrying operation
 // Retry implements internal busy loop until one of the following conditions occurs:
 // - deadline was cancelled or deadlined
 // - retry operation returned nil as error
 // Note that in case of prepared statements call to Prepare() must be made
 // inside the function body.
 err := c.Retry(ctx, false,
     func(ctx context.Context, s table.Session) (err error) {
         res, err = s.Execute(...)
         return
     },
 )
```

That is, instead of manual creation of `table.Session`, we give a
`table.Client` such responsibility. It holds instances of active sessions and
"pings" them periodically to keep them alive.

## Credentials <a name="Credentials"></a>

There are different variants to get `ydb.Credentials` object to get authorized.
Usage examples can be found [here](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth).

## Examples <a name="examples"></a>

More examples are listed in [examples](https://github.com/ydb-platform/ydb-go-examples) repository.

