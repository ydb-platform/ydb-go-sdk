package indexed

// Value is a type scan destination of ydb values
// This is a proxy type for preparing go1.18 type set constrains such as
// type Value interface {
//   ~int8 | ~int64 | ~string
// }
type Value interface{}
