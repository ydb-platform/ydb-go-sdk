<<<<<<< kikimr/public/sdk/go/ydb/yandex_specific/solomon/solomon.go
=======
package solomon

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	common "github.com/yandex-cloud/ydb-go-sdk/v2/adapters/metrics"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"a.yandex-team.ru/library/go/core/metrics"
)

type gauge struct {
	registry metrics.Registry
	name string
	g metrics.Gauge
}

func (g *gauge) With(tags map[string]string) common.Gauge {
	return &gauge{
		registry: g.registry,
		name:     g.name,
		g:        g.registry.GaugeVec(g.name, func() (labels []string) {
			for label := range tags {
				labels = append(labels, label)
			}
			return
		}()).With(tags),
	}
}

func (g *gauge) Inc() {
	g.g.Add(1)
}

func (g *gauge) Dec() {
	g.g.Add(-1)
}

func (g *gauge) Set(value float64) {
	g.g.Set(value)
}

type config struct {
	registry metrics.Registry
}

func (c *config) Gauge(name string) common.Gauge {
	return &gauge{
		registry: c.registry,
		name: name,
		g: c.registry.Gauge(name),
	}
}

// DriverTrace makes DriverTrace with solomon metrics publishing
func DriverTrace(registry metrics.Registry) ydb.DriverTrace {
	return common.DriverTrace(
		&config{
			registry: registry,
		},
	)
}

// ClientTrace makes table.ClientTrace with solomon metrics publishing
func ClientTrace(registry metrics.Registry) table.ClientTrace {
	return common.ClientTrace(
		&config{
			registry: registry,
		},
	)
}

// SessionPoolTrace makes table.SessionPoolTrace with solomon metrics publishing
func SessionPoolTrace(registry metrics.Registry) table.SessionPoolTrace {
	return common.SessionPoolTrace(
		&config{
			registry: registry,
		},
	)
}
>>>>>>> kikimr/public/sdk/go/ydb/yandex_specific/solomon/solomon.go
