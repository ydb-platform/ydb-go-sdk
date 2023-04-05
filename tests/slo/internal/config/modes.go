package config

type AppMode int

const (
	CreateMode AppMode = iota
	CleanupMode
	RunMode
)
