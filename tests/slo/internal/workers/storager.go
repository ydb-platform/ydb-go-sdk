package workers

import (
	"context"
	"slo/internal/generator"
)

type Storager interface {
	Read(context.Context, generator.EntryID) (generator.Entry, error)
	Write(context.Context, generator.Entry) error
}
