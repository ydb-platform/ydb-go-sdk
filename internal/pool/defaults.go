package pool

const DefaultLimit = 50

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
	OnChange: func(info ChangeInfo) {},
}
