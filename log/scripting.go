package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scripting(log Logger, details trace.Details) (t trace.Scripting) {
	if details&trace.ScriptingEvents != 0 {
		log = log.WithName(`scripting`)
		t.OnExecute = func(info trace.ExecuteStartInfo) func(trace.ExecuteDoneInfo) {
			log.Debugf(`execute start`)
			start := time.Now()
			return func(info trace.ExecuteDoneInfo) {
				if info.Error == nil {
					log.Debugf(`execute done {latency:"%s",resultSetCount:%v,resultSetErr:"%v""}`,
						time.Since(start),
						info.Result.ResultSetCount(),
						info.Result.Err(),
					)
				} else {
					log.Errorf(`execute failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
		t.OnExplain = func(info trace.ExplainQueryStartInfo) func(doneInfo trace.ExplainQueryDoneInfo) {
			log.Debugf(`explain start`)
			start := time.Now()
			return func(info trace.ExplainQueryDoneInfo) {
				if info.Error == nil {
					log.Debugf(`explain done {latency:"%s",ast:%v,plan:%v"}`,
						time.Since(start),
						info.AST,
						info.Plan,
					)
				} else {
					log.Errorf(`explain failed {latency:"%s",error:"%s"}`,
						time.Since(start),
						info.Error,
					)
				}
			}
		}
	}
	return t
}
