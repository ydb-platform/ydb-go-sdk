package bind

type Bindings struct {
	TablePathPrefix string
	AllowBindParams bool
}

type Binding func(b *Bindings)

func (b Bindings) Enabled() bool {
	if b.TablePathPrefix != "" {
		return true
	}
	if b.AllowBindParams {
		return true
	}
	return false
}

func WithTablePathPrefix(tablePathPrefix string) Binding {
	return func(b *Bindings) {
		b.TablePathPrefix = tablePathPrefix
	}
}

func WithAutoBindParams() Binding {
	return func(b *Bindings) {
		b.AllowBindParams = true
	}
}
