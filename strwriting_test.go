package benchmark_test

import (
	"bufio"
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

const strWritingSchema = `message test {
	required binary word (STRING);
}`

func BenchmarkStringWriting(b *testing.B) {
	var (
		words     []string
		byteWords [][]byte
	)

	prefix := "strwr_"

	f, err := os.Open("testdata/words.txt")
	if err != nil {
		b.Fatalf("Opening words.txt failed: %v", err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		words = append(words, s.Text())
		byteWords = append(byteWords, []byte(s.Text()))
	}

	b.ResetTimer()

	b.Run("parquet_go_floor_reflection", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				schemaDef, err := parquetschema.ParseSchemaDefinition(strWritingSchema)
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
					Word string `parquet:"word"`
				}

				for _, word := range words {
					stu := record{
						Word: word,
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
				schemaDef, err := parquetschema.ParseSchemaDefinition(strWritingSchema)
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

				for _, word := range words {
					r := strRecord(word)
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
				schemaDef, err := parquetschema.ParseSchemaDefinition(strWritingSchema)
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

				for _, word := range byteWords {
					stu := map[string]interface{}{
						"word": word,
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

	b.Run("parquet_go_lowlevel_disabledict", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			func() {
				parquetFilename := prefix + "parquet_go_lowlevel_nodict.parquet"

				w, err := os.OpenFile(parquetFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					b.Fatalf("Opening %s failed: %v", parquetFilename, err)
				}

				defer w.Close()

				fw := goparquet.NewFileWriter(w, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY))
				byteArrayStore, err := goparquet.NewByteArrayStore(
					parquet.Encoding_PLAIN,
					false, // this indicates that no dictionary shall be used.
					&goparquet.ColumnParameters{
						LogicalType: &parquet.LogicalType{
							STRING: parquet.NewStringType(),
						},
						ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
					},
				)
				if err != nil {
					b.Fatalf("NewByteArrayStore failed: %v", err)
				}
				fw.AddColumn("word", goparquet.NewDataColumn(byteArrayStore, parquet.FieldRepetitionType_REQUIRED))

				for _, word := range byteWords {
					stu := map[string]interface{}{
						"word": word,
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
					Word string `parquet:"name=word, type=BYTE_ARRAY, encoding=PLAIN, convertedtype=UTF8"`
				}

				//write
				pw, err := writer.NewParquetWriterFromWriter(w, new(record), 4)
				if err != nil {
					b.Fatalf("Can't create parquet writer: %v", err)
				}

				pw.CompressionType = parquet2.CompressionCodec_SNAPPY

				for _, word := range words {
					stu := record{
						Word: word,
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
					schema.MustPrimitive(schema.NewPrimitiveNodeLogical("word", parquet3.Repetitions.Required, &schema.StringLogicalType{}, parquet3.Types.ByteArray, 0, 0)),
				}, 0)

				pw := file.NewParquetWriter(w, sc, file.WithWriterProps(parquet3.NewWriterProperties(parquet3.WithCompression(compress.Codecs.Snappy))))
				defer pw.Close()

				rg := pw.AppendRowGroup()

				col, err := rg.NextColumn()
				if err != nil {
					b.Fatalf("NextColumn failed: %v", err)
				}

				wordCol, ok := col.(*file.ByteArrayColumnChunkWriter)
				if !ok {
					b.Fatalf("couldn't assert third column which is %T", col)
				}

				for _, word := range words {
					if _, err := wordCol.WriteBatch([]parquet3.ByteArray{[]byte(word)}, nil, nil); err != nil {
						b.Fatalf("WriteBatch failed: %v", err)
					}
				}

				wordCol.Close()

				defer rg.Close()
			}()
		}
	})
}

type strRecord string

func (r strRecord) MarshalParquet(obj interfaces.MarshalObject) error {
	obj.AddField("word").SetByteArray([]byte(r))
	return nil
}
