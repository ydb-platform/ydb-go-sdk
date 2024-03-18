package pool

const (
	DefaultMaxSize        = 50
	DefaultMinSize        = 0
	DefaultProducersCount = 1
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
	OnProduce: func(info *ProduceStartInfo) func(info *ProduceDoneInfo) {
		return func(info *ProduceDoneInfo) {
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
	OnSpawn: func(info *SpawnStartInfo) func(info *SpawnDoneInfo) {
		return func(info *SpawnDoneInfo) {
		}
	},
	OnWant: func(info *WantStartInfo) func(info *WantDoneInfo) {
		return func(info *WantDoneInfo) {
		}
	},
}
