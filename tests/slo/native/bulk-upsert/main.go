package main

import (
	"context"

	"slo/internal/framework"
)

func main() {
	framework.Run(func(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
		return NewStorage(ctx, fw)
	})
}
