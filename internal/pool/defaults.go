package pool

import "time"

const (
	DefaultLimit = 50

	// defaultCreateRetryDelay specifies amount of time that spawner will wait
	// before retrying unsuccessful item create.
	defaultCreateRetryDelay = 500 * time.Millisecond

	// defaultSpawnGoroutinesNumber specifies amount of spawnItems goroutines.
	// Having more than one spawner can potentially decrease warm-up time
	// and connections re-establishment time after connectivity failure.
	// Too high value will result in frequent connection establishment
	// attempts (see defaultCreateRetryDelay) during connectivity
	// issues which in some cases might not be desirable.
	defaultSpawnGoroutinesNumber = 2
)

var defaultTrace = &Trace{
	OnNew: func(info *NewStartInfo) func(info *NewDoneInfo) {
		return func(info *NewDoneInfo) {
		}
	},
	OnClose: func(info *CloseStartInfo) func(info *CloseDoneInfo) {
		return func(info *CloseDoneInfo) {
		}
	},
	OnTry: func(info *TryStartInfo) func(info *TryDoneInfo) {
		return func(info *TryDoneInfo) {
		}
	},
	OnWith: func(info *WithStartInfo) func(info *WithDoneInfo) {
		return func(info *WithDoneInfo) {
		}
	},
	OnPut: func(info *PutStartInfo) func(info *PutDoneInfo) {
		return func(info *PutDoneInfo) {
		}
	},
	OnGet: func(info *GetStartInfo) func(info *GetDoneInfo) {
		return func(info *GetDoneInfo) {
		}
	},
	OnChange: func(info ChangeInfo) {},
}
