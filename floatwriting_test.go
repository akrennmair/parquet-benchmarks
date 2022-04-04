package benchmark_test

import (
	"math/rand"
	"os"
	"testing"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	parquet2 "github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const sparseFloat64Schema = `message msg {
	required group data (LIST) {
		repeated group list {
			optional double element;
		}
	}	
}`

func BenchmarkSparseFloat64Writing(b *testing.B) {
	testData := [][]*float64{}

	for i := 0; i < 100000; i++ {
		lineSize := rand.Intn(20)
		line := make([]*float64, lineSize)
		for idx := range line {
			if idx == rand.Intn(20) {
				x := rand.Float64()
				line[idx] = &x
			}
		}
		testData = append(testData, line)
	}

	b.ResetTimer()

	b.Run("parquet_go_lowlevel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(sparseFloat64Schema)
				if err != nil {
					b.Fatalf("Parsing schema definition failed: %v", err)
				}

				w, err := os.Create("float64wr_parquet_go_lowlevel.parquet")
				if err != nil {
					b.Fatalf("Create failed: %v", err)
				}

				defer w.Close()

				fw := goparquet.NewFileWriter(w, goparquet.WithSchemaDefinition(schemaDef),
					goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY))

				for _, line := range testData {
					obj := createLineObject(line)
					if err = fw.AddData(obj); err != nil {
						b.Fatalf("Write error: %v", err)
					}
				}

				if err := fw.Close(); err != nil {
					b.Fatalf("Closing parquet writer failed: %v", err)
				}
			}()
		}
	})

	b.Run("xitongsys_parquet_go", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				w, err := os.Create("float64wr_xitongsys.parquet")
				if err != nil {
					b.Fatalf("Create failed: %v", err)
				}

				defer w.Close()

				type record struct {
					Data []*float64 `parquet:"name=data, type=LIST, convertedtype=LIST, valuetype=DOUBLE"`
				}

				//write
				pw, err := writer.NewParquetWriterFromWriter(w, new(record), 1)
				if err != nil {
					b.Fatalf("Can't create parquet writer: %v", err)
				}

				pw.CompressionType = parquet2.CompressionCodec_SNAPPY

				for _, list := range testData {
					rec := record{
						Data: list,
					}

					if err = pw.Write(rec); err != nil {
						b.Fatalf("Write error: %v", err)
					}
				}

				if err = pw.WriteStop(); err != nil {
					b.Fatalf("WriteStop error: %v", err)
				}
				w.Close()
			}()
		}
	})
}

func createLineObject(line []*float64) map[string]interface{} {
	list := []map[string]interface{}{}

	for _, fp := range line {
		if fp != nil {
			list = append(list, map[string]interface{}{"element": *fp})
		} else {
			list = append(list, nil)
		}
	}

	return map[string]interface{}{
		"data": map[string]interface{}{
			"list": list,
		},
	}
}
