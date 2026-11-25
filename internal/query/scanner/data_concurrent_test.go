package scanner

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestDataConcurrentNamedScans(t *testing.T) {
	const width = 64
	columns := make([]*Ydb.Column, width+1)
	values := make([]value.Value, len(columns))
	for column := range columns {
		columns[column] = &Ydb.Column{Name: fmt.Sprintf("column_%d", column)}
		values[column] = value.Int32Value(int32(column + 1))
	}
	columns[width].Name = columns[0].GetName()
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			data := NewDecodedData(columns, values)
			if direct {
				data = NewDirectData(columns, &mockDirectRow{values: values})
			}
			const workers = 16
			var ready, done sync.WaitGroup
			ready.Add(workers)
			done.Add(workers)
			start := make(chan struct{})
			errors := make(chan error, workers)
			for range workers {
				go func() {
					defer done.Done()
					ready.Done()
					<-start
					for range 20 {
						var first, last int32
						err := Named(data).ScanNamed(NamedRef("column_63", &last), NamedRef("column_0", &first))
						if err != nil {
							errors <- err

							return
						}
						if first != 1 || last != width {
							errors <- fmt.Errorf("values = (%d, %d), want (1, %d)", first, last, width)

							return
						}
					}
				}()
			}
			ready.Wait()
			close(start)
			done.Wait()
			close(errors)
			for err := range errors {
				require.NoError(t, err)
			}
		})
	}
}
