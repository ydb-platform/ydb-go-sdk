Ydbgen will be removed at the next major release.
For following reasons:
- Appeared new scanner API 
- Deleted ydb.time
- Small usages among private Yandex users  

Users can use new scanner API.  
For dml queries will be created special struct `Builder` which allow to do it more convenient.

Migrations cases are described below:

### Scan structs
```go
res.Scan(&t.Name, &t.ID, &t.Description)
```
### Scan complex yql types (list)  
```go
type exampleList struct {
    elems []string
}

func (l *exampleList) UnmarshalYDB(res ydb.RawValue) error {
    n := res.ListIn()
    l.elems = make([]string, n)
    for i := 0; i < n; i++ {
        res.ListItem(i)
        l.elems[i] = res.String()
    }
    res.ListOut()
    return res.Err()
}
```

### Scan own data type (it was impossible for ydbgen)
```go
type MyProto pb.SomeMessage
func (p *MyProto) func UnmarshallYDB(v ydb.RawValue) error {
    return proto.Unmarshall(p, v.JsonDocument())
}
var myProto MyProto
res.Scan(&t.ID, &t.Name, &t.Description, &myProto)
```
### Prepare query parameters 
```go
table.NewQueryParameters(
    table.ValueParam("$name", types.UTF8Value(name)),
    table.ValueParam("$description", types.UTF8Value(name)),
)
```
### Insert, update, delete queries
```go
stmt.Execute(ctx, writeTx,
    table.NewQueryParameters(
        table.ValueParam("$x", ydb.StructValue(
                                    ydb.StructFieldValue("name", ydb.UTF8Value(name)),
                                    ydb.StructFieldValue("id", ydb.Int32Value(id)), 
									ydb.StructFieldValue("description", ydb.UTF8Value(description))
                                ),
                        )),
)
```
### Scan date and interval 
```go
var date time.Time
err := res.Scan(&date)
//error handing ...
```
