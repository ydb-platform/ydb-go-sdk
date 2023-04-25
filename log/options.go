package log

type Option interface {
	applyHolderOption(l *wrapper)
}

type coloringSimpleOption bool

func (coloring coloringSimpleOption) applySimpleOption(l *simpleLogger) {
	l.coloring = bool(coloring)
}

func WithColoring() simpleLoggerOption {
	return coloringSimpleOption(true)
}

type minLevelSimpleOption Level

func (minLevel minLevelSimpleOption) applySimpleOption(l *simpleLogger) {
	l.minLevel = Level(minLevel)
}

func WithMinLevel(level Level) simpleLoggerOption {
	return minLevelSimpleOption(level)
}

type namespaceOption string

func (namespace namespaceOption) applyHolderOption(l *wrapper) {
	l.namespace = append(l.namespace, string(namespace))
}

func WithNamespace(namespace string) namespaceOption {
	return namespaceOption(namespace)
}

type logQueryOption bool

func (logQuery logQueryOption) applySimpleOption(l *simpleLogger) {
	l.logQuery = bool(logQuery)
}

func (logQuery logQueryOption) applyHolderOption(l *wrapper) {
	l.logQuery = bool(logQuery)
}

func WithLogQuery() logQueryOption {
	return true
}
