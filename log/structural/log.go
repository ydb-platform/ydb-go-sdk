package structural

import (
	"fmt"
	"time"
)

type Logger interface {
	// Trace returns log record with TRACE level
	Trace() Record
	// Debug returns log record with DEBUG level
	Debug() Record
	// Info returns log record with INFO level
	Info() Record
	// Warn returns log record with WARN level
	Warn() Record
	// Error returns log record with ERROR level
	Error() Record
	// Fatal returns log record with FATAL level
	Fatal() Record

	// WithName returns sublogger with added name
	WithName(name string) Logger

	// Object returns empty Record to be used in Record.Object.
	// Record.Message should be a no-op on Records created with Logger.Object.
	Object() Record
	// Array returns empty Array to be used in Record.Array
	Array() Array
}

type Array interface {
	// Object adds map- or struct-like item. It is intended that value is
	// created with Logger.Object method.
	Object(value Record) Array
	// Array adds array-like item
	Array(value Array) Array

	String(value string) Array
	Strings(value []string) Array
	Stringer(value fmt.Stringer) Array

	Duration(value time.Duration) Array

	Int(value int) Array
	Int8(value int8) Array
	Int16(value int16) Array
	Int32(value int32) Array
	Int64(value int64) Array

	Uint(value uint) Array
	Uint8(value uint8) Array
	Uint16(value uint16) Array
	Uint32(value uint32) Array
	Uint64(value uint64) Array

	Float32(value float32) Array
	Float64(value float64) Array

	Bool(value bool) Array

	Any(value interface{}) Array
}

type Record interface {
	// Object adds map- or struct-like field. It is intended that value is
	// created with Logger.Object method.
	Object(key string, value Record) Record
	// Array adds array-like field
	Array(key string, value Array) Record

	String(key string, value string) Record
	Strings(key string, value []string) Record
	Stringer(key string, value fmt.Stringer) Record

	Duration(key string, value time.Duration) Record

	Int(key string, value int) Record
	Int8(key string, value int8) Record
	Int16(key string, value int16) Record
	Int32(key string, value int32) Record
	Int64(key string, value int64) Record

	Uint(key string, value uint) Record
	Uint8(key string, value uint8) Record
	Uint16(key string, value uint16) Record
	Uint32(key string, value uint32) Record
	Uint64(key string, value uint64) Record

	Float32(key string, value float32) Record
	Float64(key string, value float64) Record

	Bool(key string, value bool) Record

	Error(value error) Record
	NamedError(key string, value error) Record

	Any(key string, value interface{}) Record

	// Logging actions:

	// Message adds text message and sends the Record. Record should be
	// disposed after call to Message(). This method should be a no-op for
	// Records created with Logger.Object().
	Message(msg string)
}
