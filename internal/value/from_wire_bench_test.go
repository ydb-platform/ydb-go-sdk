package value

import (
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

var benchmarkDecodedValue Value

// BenchmarkValueDecode covers every value type used by the differential test.
// Both paths include decoding and the SDK's CastTo operation.
func BenchmarkValueDecode(b *testing.B) {
	for name, original := range wireTestValues() {
		pb := ToYDB(original)
		data, err := proto.Marshal(pb.GetValue())
		if err != nil {
			b.Fatal(err)
		}
		decodedType := types.TypeFromYDB(pb.GetType())
		b.Run(name, func(b *testing.B) {
			b.Run("protobuf", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var message Ydb.Value
					if err := proto.Unmarshal(data, &message); err != nil {
						b.Fatal(err)
					}
					var dst Value
					if err := CastTo(FromYDB(pb.GetType(), &message), &dst); err != nil {
						b.Fatal(err)
					}
					benchmarkDecodedValue = dst
				}
			})
			b.Run("wire", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, err := FromWire(decodedType, data)
					if err != nil {
						b.Fatal(err)
					}
					var dst Value
					if err := CastTo(v, &dst); err != nil {
						b.Fatal(err)
					}
					benchmarkDecodedValue = dst
				}
			})
		})
	}
}
