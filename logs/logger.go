package logs

type Logger interface {
	Log(opts Options, msg string, fields ...Field)
}

type Options struct {
	Lvl   Level
	Scope []string
}
