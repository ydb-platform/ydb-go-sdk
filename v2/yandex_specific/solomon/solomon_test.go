<<<<<<< kikimr/public/sdk/go/ydb/yandex_specific/solomon/solomon_test.go
=======
package solomon

import (
	"a.yandex-team.ru/library/go/core/metrics/solomon"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_WithTagsCached(t *testing.T) {
	c := &config{
		registry: solomon.NewRegistry(solomon.NewRegistryOpts()),
	}
	c.Gauge("test").With(map[string]string{
		"key1": "value1",
		"key2": "value2",
	}).Set(555)
	c.Gauge("test").With(map[string]string{
		"key1": "value1",
		"key2": "value2",
	}).Inc()
	c.Gauge("test").With(map[string]string{
		"key3": "value3",
		"key4": "value4",
	}).Inc()
	g, ok := c.Gauge("test").With(map[string]string{
		"key1": "value1",
		"key2": "value2",
	}).(*gauge).g.(*solomon.Gauge)
	require.True(t, ok)
	b, err := g.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "{\"type\":\"DGAUGE\",\"labels\":{\"key1\":\"value1\",\"key2\":\"value2\",\"sensor\":\"test\"},\"value\":556}", string(b))
}
>>>>>>> kikimr/public/sdk/go/ydb/yandex_specific/solomon/solomon_test.go
