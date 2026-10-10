// Package wirevalue reads YDB Value messages as borrowed views of protobuf
// bytes. Query retains the response frame for each row. Primitive fields use
// numbers generated from the pinned YDB descriptors; collection iterators
// traverse the same bytes without allocating protobuf Value messages.
package wirevalue
