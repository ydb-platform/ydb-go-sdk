package logs

type Logger interface {
	Log(opts Options, msg string, fields ...Field)

	// Enabled tells whether the loggin level is enabled by the logger.
	// It can be used for optimization to prevent expensive field initialization
	// for disabled messages. If the Logger does not support this feature,
	// its Enabled() method should always return true.
	// Logger should expect calls to Log() even if it is disabled.
	Enabled(lvl Level) bool
}

type Options struct {
	Lvl   Level
	Scope []string
}
