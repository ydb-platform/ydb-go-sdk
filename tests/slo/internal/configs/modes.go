package configs

type AppMode int

const (
	CreateMode AppMode = iota
	CleanupMode
	RunMode
)
