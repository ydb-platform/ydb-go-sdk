package config

type LockConfig struct {
	count         uint64
	timeoutMillis uint64
	data          []byte
}

type LockOption func(l *LockConfig)

func NewLockConfig() *LockConfig {
	return &LockConfig{
		timeoutMillis: 1000,
	}
}

func (l *LockConfig) Count() uint64 {
	return l.count
}

func (l *LockConfig) TimeoutMillis() uint64 {
	return l.timeoutMillis
}

func (l *LockConfig) Data() []byte {
	return l.data
}

func WithLockCount(count uint64) LockOption {
	return func(l *LockConfig) {
		l.count = count
	}
}

func WithLockTimeoutMillis(timeoutMillis uint64) LockOption {
	return func(l *LockConfig) {
		l.timeoutMillis = timeoutMillis
	}
}

func WithLockData(data []byte) LockOption {
	return func(l *LockConfig) {
		l.data = data
	}
}

type UnlockConfig struct {
	count uint64
}

type UnlockOption = func(l *UnlockConfig)

func NewUnlockConfig() *UnlockConfig {
	return &UnlockConfig{}
}

func (l *UnlockConfig) Count() uint64 {
	return l.count
}

func WithUnlockCount(count uint64) UnlockOption {
	return func(l *UnlockConfig) {
		l.count = count
	}
}

type KeepAliveOnceConfig struct {
}

type KeepAliveOnceOption func(k *KeepAliveOnceConfig)
