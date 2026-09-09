package tracefuzz

import (
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func typedNil(t reflect.Type) (reflect.Value, bool) {
	if v, ok := typedNilCommon(t); ok {
		return v, true
	}

	return typedNilQuery(t)
}

func typedNilCommon(t reflect.Type) (reflect.Value, bool) {
	switch t {
	case reflect.TypeFor[trace.SessionInfo]():
		var s *fuzzSession

		return reflect.ValueOf((trace.SessionInfo)(s)), true
	case reflect.TypeFor[trace.TxInfo]():
		var tx *fuzzTx

		return reflect.ValueOf((trace.TxInfo)(tx)), true
	case reflect.TypeFor[trace.EndpointInfo]():
		var e *fuzzEndpoint

		return reflect.ValueOf((trace.EndpointInfo)(e)), true
	case reflect.TypeFor[trace.ConnState]():
		var s *fuzzConnState

		return reflect.ValueOf((trace.ConnState)(s)), true
	case reflect.TypeFor[trace.Call]():
		var c *fuzzCall

		return reflect.ValueOf((trace.Call)(c)), true
	case reflect.TypeFor[trace.Issue]():
		var i *fuzzIssue

		return reflect.ValueOf((trace.Issue)(i)), true
	default:
		return reflect.Value{}, false
	}
}

func typedNilQuery(t reflect.Type) (reflect.Value, bool) {
	switch t {
	case reflect.TypeFor[trace.TableQueryParameters]():
		var p *fuzzTableQueryParameters

		return reflect.ValueOf((trace.TableQueryParameters)(p)), true
	case reflect.TypeFor[trace.TableDataQuery]():
		var q *fuzzTableDataQuery

		return reflect.ValueOf((trace.TableDataQuery)(q)), true
	case reflect.TypeFor[trace.TableResultErr]():
		var r *fuzzTableResultErr

		return reflect.ValueOf((trace.TableResultErr)(r)), true
	case reflect.TypeFor[trace.TableResult]():
		var r *fuzzTableResult

		return reflect.ValueOf((trace.TableResult)(r)), true
	case reflect.TypeFor[trace.ScriptingQueryParameters]():
		var p *fuzzScriptingQueryParameters

		return reflect.ValueOf((trace.ScriptingQueryParameters)(p)), true
	case reflect.TypeFor[trace.ScriptingResultErr]():
		var r *fuzzScriptingResultErr

		return reflect.ValueOf((trace.ScriptingResultErr)(r)), true
	case reflect.TypeFor[trace.ScriptingResult]():
		var r *fuzzScriptingResult

		return reflect.ValueOf((trace.ScriptingResult)(r)), true
	default:
		return reflect.Value{}, false
	}
}
