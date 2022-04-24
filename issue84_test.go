package benchmark_test

import (
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
	parquet4 "github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress/snappy"
	parquet2 "github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func BenchmarkIssue84(b *testing.B) {
	numRecords := 1000
	prefix := "issue84_"

	b.Run("parquet_go_floor_reflection", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(
					`message test {
						required binary format (STRING);
						required int32 data_type;
						required binary country (STRING);
					}`)
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
					Format   string `parquet:"format"`
					DataType int32  `parquet:"data_type"`
					Country  string `parquet:"country"`
				}

				for i := 0; i < numRecords; i++ {
					stu := record{
						Format:   "Test",
						DataType: 1,
						Country:  "IN",
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
				schemaDef, err := parquetschema.ParseSchemaDefinition(
					`message test {
						required binary format (STRING);
						required int32 data_type;
						required binary country (STRING);
					}`)
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

				for i := 0; i < numRecords; i++ {
					stu := &marshalRecord{
						Format:   "Test",
						DataType: 1,
						Country:  "IN",
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

	b.Run("parquet_go_lowlevel", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(
					`message test {
						required binary format (STRING);
						required int32 data_type;
						required binary country (STRING);
					}`)
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

				for i := 0; i < numRecords; i++ {
					stu := map[string]interface{}{
						"format":    []byte("Test"),
						"data_type": int32(1),
						"country":   []byte("IN"),
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
					Format   string `parquet:"name=format, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
					DataType int32  `parquet:"name=data_type, type=INT32, encoding=PLAIN"`
					Country  string `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
				}

				//write
				pw, err := writer.NewParquetWriterFromWriter(w, new(record), 4)
				if err != nil {
					b.Fatalf("Can't create parquet writer: %v", err)
				}

				pw.CompressionType = parquet2.CompressionCodec_SNAPPY

				for i := 0; i < numRecords; i++ {
					stu := record{
						Format:   "Test",
						DataType: 1,
						Country:  "IN",
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
					schema.MustPrimitive(schema.NewPrimitiveNodeLogical("format", parquet3.Repetitions.Required, &schema.StringLogicalType{}, parquet3.Types.ByteArray, 0, 0)),
					schema.MustPrimitive(schema.NewPrimitiveNode("data_type", parquet3.Repetitions.Required, parquet3.Types.Int32, 0, 0)),
					schema.MustPrimitive(schema.NewPrimitiveNodeLogical("country", parquet3.Repetitions.Required, &schema.StringLogicalType{}, parquet3.Types.ByteArray, 0, 0)),
				}, 0)

				pw := file.NewParquetWriter(w, sc, file.WithWriterProps(parquet3.NewWriterProperties(parquet3.WithCompression(compress.Codecs.Snappy))))
				defer pw.Close()

				rg := pw.AppendRowGroup()

				col, err := rg.NextColumn()
				if err != nil {
					b.Fatalf("NextColumn failed: %v", err)
				}

				formatCol, ok := col.(*file.ByteArrayColumnChunkWriter)
				if !ok {
					b.Fatalf("couldn't assert first column which is %T", col)
				}

				for i := 0; i < numRecords; i++ {
					if _, err := formatCol.WriteBatch([]parquet3.ByteArray{[]byte("Test")}, nil, nil); err != nil {
						b.Fatalf("WriteBatch failed: %v", err)
					}
				}
				formatCol.Close()

				col, err = rg.NextColumn()
				if err != nil {
					b.Fatalf("NextColumn failed: %v", err)
				}

				dataTypeCol, ok := col.(*file.Int32ColumnChunkWriter)
				if !ok {
					b.Fatalf("couldn't assert second column which is %T", col)
				}

				for i := 0; i < numRecords; i++ {
					if _, err := dataTypeCol.WriteBatch([]int32{1}, nil, nil); err != nil {
						b.Fatalf("WriteBatch failed: %v", err)
					}
				}

				dataTypeCol.Close()

				col, err = rg.NextColumn()
				if err != nil {
					b.Fatalf("NextColumn failed: %v", err)
				}

				countryCol, ok := col.(*file.ByteArrayColumnChunkWriter)
				if !ok {
					b.Fatalf("couldn't assert third column which is %T", col)
				}

				for i := 0; i < numRecords; i++ {
					if _, err := countryCol.WriteBatch([]parquet3.ByteArray{[]byte("IN")}, nil, nil); err != nil {
						b.Fatalf("WriteBatch failed: %v", err)
					}
				}

				countryCol.Close()

				defer rg.Close()
			}()
		}
	})

	b.Run("segmentio_parquet_go", func(b *testing.B) {
		type record struct {
			Format   string `parquet:"format"`
			DataType int32  `parquet:"data_type"`
			Country  string `parquet:"country"`
		}

		for n := 0; n < b.N; n++ {
			func() {
				parquetFilename := prefix + "segmentio.parquet"

				f, err := os.Create(parquetFilename)
				if err != nil {
					b.Fatalf("Creating %s failed: %v", parquetFilename, err)
				}

				wr := parquet4.NewWriter(f, parquet4.SchemaOf(new(record)), parquet4.Compression(&snappy.Codec{}))

				for i := 0; i < numRecords; i++ {
					if err := wr.Write(&record{
						Format:   "Test",
						DataType: 1,
						Country:  "IN",
					}); err != nil {
						b.Fatalf("Write failed: %v", err)
					}
				}

				if err := wr.Close(); err != nil {
					b.Fatalf("Closing parquet writer failed: %v", err)
				}
			}()
		}
	})
}

type marshalRecord struct {
	Format   string
	DataType int32
	Country  string
}

func (r *marshalRecord) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("format").SetByteArray([]byte(r.Format))
	obj.AddField("data_type").SetInt32(r.DataType)
	obj.AddField("country").SetByteArray([]byte(r.Country))
	return nil
}
