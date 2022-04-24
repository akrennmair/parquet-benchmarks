package benchmark_test

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"testing"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	parquet4 "github.com/segmentio/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func BenchmarkInt32Reading(b *testing.B) {
	numRecords := 1000000
	data := make([]int32, numRecords)

	b.Run("high_card", func(b *testing.B) {
		for i := range data {
			data[i] = rand.Int31()
		}
		b.ResetTimer()

		benchmarkInt32Reading(b, data, "int32_high_card_")
	})

	b.Run("low_card", func(b *testing.B) {
		cardinality := int32(1328)

		for i := range data {
			data[i] = rand.Int31n(cardinality)
		}
		b.ResetTimer()

		benchmarkInt32Reading(b, data, "int32_low_card_")
	})
}

func benchmarkInt32Reading(b *testing.B, data []int32, prefix string) {
	schemaDef, err := parquetschema.ParseSchemaDefinition(int32WritingSchema)
	if err != nil {
		b.Fatalf("Parsing schema definition failed: %v", err)
	}

	parquetFilename := prefix + "testdata.parquet"

	fw, err := floor.NewFileWriter(parquetFilename,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		b.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	for _, num := range data {
		stu := myInt32Record{
			Foo: num,
		}
		if err = fw.Write(stu); err != nil {
			b.Fatalf("Write error: %v", err)
		}
	}

	if err := fw.Close(); err != nil {
		b.Fatalf("Closing parquet writer failed: %v", err)
	}

	b.ResetTimer()

	b.Run("parquet_lowlevel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				f, err := os.Open(parquetFilename)
				if err != nil {
					b.Fatalf("Opening file failed: %v", err)
				}

				r, err := goparquet.NewFileReader(f)
				if err != nil {
					b.Fatalf("Reading parquet file failed: %v", err)
				}

				for {
					_, err := r.NextRow()
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatalf("NextRow returned error: %v", err)
					}
				}
			}()
		}
	})

	b.Run("parquet_floor_reflection", func(b *testing.B) {
		type reflectRecord struct {
			Foo int32 `parquet:"foo"`
		}

		for i := 0; i < b.N; i++ {
			func() {
				r, err := floor.NewFileReader(parquetFilename)
				if err != nil {
					b.Fatalf("Opening file failed: %v", err)
				}

				for r.Next() {
					var rec reflectRecord
					if err := r.Scan(&rec); err != nil {
						b.Fatalf("Scan failed: %v", err)
					}
				}

				r.Close()
			}()
		}
	})

	b.Run("parquet_floor_unmarshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				r, err := floor.NewFileReader(parquetFilename)
				if err != nil {
					b.Fatalf("Opening file failed: %v", err)
				}

				for r.Next() {
					var rec myInt32Record
					if err := r.Scan(&rec); err != nil {
						b.Fatalf("Scan failed: %v", err)
					}
				}

				r.Close()
			}()
		}
	})

	b.Run("xitongsys", func(b *testing.B) {
		type record struct {
			Foo int32 `parquet:"name=foo, type=INT32"`
		}
		for i := 0; i < b.N; i++ {
			func() {
				fr, err := local.NewLocalFileReader(parquetFilename)
				if err != nil {
					b.Fatalf("Can't open file: %v", err)
				}

				pr, err := reader.NewParquetReader(fr, new(record), 1)
				if err != nil {
					b.Fatalf("Creating parquet reader failed: %v", err)

				}

				num := int(pr.GetNumRows())

				for num > 0 {
					sliceSize := 100
					if num < sliceSize {
						sliceSize = num
					}
					rec := make([]record, sliceSize)
					if err := pr.Read(&rec); err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatalf("Read failed: %v", err)
					}

					num -= sliceSize
				}

				pr.ReadStop()
				fr.Close()
			}()
		}
	})

	b.Run("segmentio", func(b *testing.B) {
		type record struct {
			Foo int32 `parquet:"foo"`
		}
		for i := 0; i < b.N; i++ {
			func() {
				f, err := os.Open(parquetFilename)
				if err != nil {
					b.Fatalf("Opening file failed: %v", err)
				}
				defer f.Close()
				r := parquet4.NewReader(f)
				for {
					var rec record
					if err := r.Read(&rec); err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatalf("Read failed: %v", err)
					}
				}
			}()
		}
	})
}

type myInt32Record struct {
	Foo int32 `parquet:"foo"`
}

func (r *myInt32Record) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	i32, err := obj.GetField("foo").Int32()
	if err != nil {
		return err
	}
	r.Foo = i32
	return nil
}
