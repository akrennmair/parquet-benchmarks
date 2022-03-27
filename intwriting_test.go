package benchmark_test

import (
	"math/rand"
	"os"
	"testing"

	parquet3 "github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/compress"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/schema"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	parquet2 "github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func BenchmarkInt32Writing(b *testing.B) {
	numRecords := 1000000

	b.Run("high_card", func(b *testing.B) {
		prefix := "int32wr_highcard_"

		data := make([]int32, numRecords)

		for i := range data {
			data[i] = rand.Int31()
		}

		benchmarkInt32Writing(b, data, prefix)
	})

	b.Run("low_card", func(b *testing.B) {
		prefix := "int32wr_lowcard_"
		cardinality := int32(1516)

		data := make([]int32, numRecords)

		for i := range data {
			data[i] = rand.Int31n(cardinality)
		}

		benchmarkInt32Writing(b, data, prefix)
	})

}

const int32WritingSchema = `message test {
	required int32 foo;
}`

func benchmarkInt32Writing(b *testing.B, data []int32, prefix string) {
	b.Run("parquet_go_floor_reflection", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(int32WritingSchema)
				if err != nil {
					b.Fatalf("Parsing schema definition failed: %v", err)
				}

				parquetFilename := prefix + "parquet_go_floor_reflection.parquet"

				fw, err := floor.NewFileWriter(parquetFilename,
					goparquet.WithSchemaDefinition(schemaDef),
					goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				)
				if err != nil {
					b.Fatalf("Opening parquet file for writing failed: %v", err)
				}

				type record struct {
					Foo int32 `parquet:"foo"`
				}

				for _, num := range data {
					stu := record{
						Foo: num,
					}
					if err = fw.Write(stu); err != nil {
						b.Fatalf("Write error: %v", err)
					}
				}

				if err := fw.Close(); err != nil {
					b.Fatalf("Closing parquet writer failed: %v", err)
				}
			}()
		}
	})

	b.Run("parquet_go_floor_marshalling", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(int32WritingSchema)
				if err != nil {
					b.Fatalf("Parsing schema definition failed: %v", err)
				}

				parquetFilename := prefix + "parquet_go_floor_marshalling.parquet"

				fw, err := floor.NewFileWriter(parquetFilename,
					goparquet.WithSchemaDefinition(schemaDef),
					goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				)
				if err != nil {
					b.Fatalf("Opening parquet file for writing failed: %v", err)
				}

				for _, num := range data {
					r := int32Record(num)
					if err = fw.Write(&r); err != nil {
						b.Fatalf("Write error: %v", err)
					}
				}

				if err := fw.Close(); err != nil {
					b.Fatalf("Closing parquet writer failed: %v", err)
				}
			}()
		}
	})

	b.Run("parquet_go_lowlevel", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(int32WritingSchema)
				if err != nil {
					b.Fatalf("Parsing schema definition failed: %v", err)
				}

				parquetFilename := prefix + "parquet_go_lowlevel.parquet"

				w, err := os.OpenFile(parquetFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					b.Fatalf("Opening %s failed: %v", parquetFilename, err)
				}

				defer w.Close()

				fw := goparquet.NewFileWriter(w, goparquet.WithSchemaDefinition(schemaDef),
					goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY))

				for _, num := range data {
					stu := map[string]interface{}{
						"foo": num,
					}
					if err = fw.AddData(stu); err != nil {
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
		for n := 0; n < b.N; n++ {
			func() {
				filename := prefix + "xitongsys_parquet_go.parquet"

				w, err := os.Create(filename)
				if err != nil {
					b.Fatalf("Can't create local file: %v", err)
				}

				type record struct {
					Foo int32 `parquet:"name=data_type, type=INT32, encoding=PLAIN"`
				}

				//write
				pw, err := writer.NewParquetWriterFromWriter(w, new(record), 4)
				if err != nil {
					b.Fatalf("Can't create parquet writer: %v", err)
				}

				pw.CompressionType = parquet2.CompressionCodec_SNAPPY

				for _, num := range data {
					stu := record{
						Foo: num,
					}
					if err = pw.Write(stu); err != nil {
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

	b.Run("apache_arrow_parquet", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				filename := prefix + "apache_arrow_parquet.parquet"
				w, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					b.Fatalf("Opening file failed: %v", err)
				}

				sc, err := schema.NewGroupNode("test", parquet3.Repetitions.Required, schema.FieldList{
					schema.MustPrimitive(schema.NewPrimitiveNode("foo", parquet3.Repetitions.Required, parquet3.Types.Int32, 0, 0)),
				}, 0)

				pw := file.NewParquetWriter(w, sc, file.WithWriterProps(parquet3.NewWriterProperties(parquet3.WithCompression(compress.Codecs.Snappy))))
				defer pw.Close()

				rg := pw.AppendRowGroup()

				col, err := rg.NextColumn()
				if err != nil {
					b.Fatalf("NextColumn failed: %v", err)
				}

				fooCol, ok := col.(*file.Int32ColumnChunkWriter)
				if !ok {
					b.Fatalf("couldn't assert foo column which is %T", col)
				}

				if _, err := fooCol.WriteBatch(data, nil, nil); err != nil {
					b.Fatalf("WriteBatch failed: %v", err)
				}

				fooCol.Close()

				defer rg.Close()
			}()
		}
	})
}

type int32Record int32

func (r int32Record) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("foo").SetInt32(int32(r))
	return nil
}
