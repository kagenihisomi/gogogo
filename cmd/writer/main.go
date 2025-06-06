package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	outputFilePath string
)

// Student struct matches the requested schema
type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
	Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Ignored int32   //without parquet tag and won't write
}

// DataFrame is a generic container for tabular data
type DataFrame[T any] struct {
	Records []T
	schema  interface{} // Used for schema inference
}

// CreateDataFrame creates a new DataFrame with the given records
func CreateDataFrame[T any](records []T) *DataFrame[T] {
	// Use a pointer to the first record as schema reference, or empty struct if no records
	var empty T
	schema := &empty

	return &DataFrame[T]{
		Records: records,
		schema:  schema,
	}
}

// WriteToLocalParquet writes the DataFrame to a local Parquet file
func (df *DataFrame[T]) WriteToLocalParquet(filePath string) error {
	// Create the file writer
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create local writer for path '%s': %w", filePath, err)
	}
	defer fw.Close()

	// Create the parquet writer
	pw, err := writer.NewParquetWriter(fw, df.schema, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write each record
	for i, record := range df.Records {
		if err := pw.Write(record); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("failed to write record at index %d: %w", i, err)
		}
	}

	// Finalize writing
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finalize parquet file: %w", err)
	}

	return nil
}

func main() {
	outputFilePath = "tmp/students.parquet"
	fmt.Printf("Creating student records and writing to: %s\n", outputFilePath)

	// Create sample student data
	students := []Student{
		{
			Name:    "Alice",
			Age:     22,
			Id:      1001,
			Weight:  65.5,
			Sex:     false,                                                             // female
			Day:     int32(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400), // Convert to days since epoch
			Ignored: 100,                                                               // This field will be ignored in the parquet file
		},
		{
			Name:    "Bob",
			Age:     23,
			Id:      1002,
			Weight:  72.5,
			Sex:     true, // male
			Day:     int32(time.Date(1999, 5, 10, 0, 0, 0, 0, time.UTC).Unix() / 86400),
			Ignored: 200,
		},
		{
			Name:    "Charlie",
			Age:     21,
			Id:      1003,
			Weight:  68.0,
			Sex:     true,
			Day:     int32(time.Date(2001, 7, 23, 0, 0, 0, 0, time.UTC).Unix() / 86400),
			Ignored: 300,
		},
	}

	// Create a DataFrame
	df := CreateDataFrame(students)

	// Create output directory if it doesn't exist
	outputDir := filepath.Dir(outputFilePath)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Printf("Failed to create output directory: %v\n", err)
			os.Exit(1)
		}
	}

	// Write to Parquet
	if err := df.WriteToLocalParquet(outputFilePath); err != nil {
		fmt.Printf("Failed to write Parquet file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully wrote %d student records to Parquet file\n", len(students))
}
