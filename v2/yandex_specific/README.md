# Yandex Specific Notes

## Design

### Motivation

This library was designed with few main ideas in mind:

1) **Absolutely** hide internal details such that knowdledge of serialization
format (protobuf objects) or network protocol (grpc).

2) On the other hand, give user ability to work with YDB storage types in
various ways (again, not showing some internal decisions which may change in
future).

3) Networking was initially implemented with an idea of using `grpc` package
and its features as little as possible – and it was payed off multiple time
during implementations of client side load balancing and other features.

4) Request results scanning was initially implemented with an eye on code
generation.

### Overview

The core `ydb` package contains common logic of network communication with YDB
– client side balancing, endpoint discovery and rpc.

Packages such `table` or `scheme` mirrors YDB's API hierarchy and provides
specific part of it.

Other packages contains tools and helpers.

Note that package `api` is special. Initially it was living inside `internal`
directory. But since `sdk/go/persqueue` was implemented as a separate library,
it was needed to use the same results of protobuf generation – and if some user
import `persqueue` and `ydb` in the same app grpc starts to panic. After that
`api` package become exported.

Please also note, that `api` package contains some customization for grpc
generation. Mostly to support generic invokation of grpc methods, but also to
provide some generic methods (for example to set up operation parameters).
See `internal/cmd` and `api/_make`.

## Bitbucket development notes

Master repository is Arcadia. But there is a bitbucket mirror
[repo](https://bb.yandex-team.ru/projects/CLOUD/repos/ydb-go/browse).
It is [synced](https://sandbox.yandex-team.ru/scheduler/14400/tasks) every 15 minutes.

So if you plan update some ydb code, the steps are as follows:
1) Checkout arcadia repo as it said [here](https://clubs.at.yandex-team.ru/arcadia/17695/).
2) Make some changes, test and commit them.
3) Ask for review, ship it.
4) Wait for merge; wait for sync with bitbucket.
5) Update ydb-go repo (for cloud-go it is `dep ensure -update bb.yandex-team.ru/cloud/ydb-go`).

## Arcadia development notes

This library uses generation of Go code from protobuf specs. That is, in case
of development/debugging processes it is helpful to have result of that
generation in the directory tree. To achieve this simply run this command:

```
$ ya make --add-result go kikimr/public/sdk/go/ydb
```

This will create symlinks inside directories to the result of generation.

Also, this step may be automated via this command:
```
$ mkdir -p junk/$USERNAME
$ ya gen-config > junk/$USERNAME/ya.conf
$ sed -i "" 's/# add_result = \[\]/add_result = ["go"]/g' junk/$USERNAME/ya.conf
```
## TODOS

- Rename types to be consistent in format like values: BoolType <-> BoolValue
  and so on.
- Support "truncated flag" in responses (no clear vision how to deal with it,
  please look at C++ library).
- Add meaningful error messages to `ydb/internal/result` instead of `TODO`.
