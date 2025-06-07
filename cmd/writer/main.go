package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	outputFilePath string
)

// Student struct matches the requested schema
type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
	Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Ignored *int32  `parquet:"name=ignored, type=INT32"`
	// Added field for record-level ETL metadata
	RecordInfo `parquet:"name=_recordinfo, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
}

type RecordInfo struct {
	RawData         string `json:"_raw_data" parquet:"name=_raw_data, type=BYTE_ARRAY, ConvertedType=UTF8"`
	RowHash         string `json:"_row_hash" parquet:"name=_row_hash, type=BYTE_ARRAY, ConvertedType=UTF8"`
	IngestTimestamp int64  `json:"_ingest_timestamp" parquet:"name=_ingest_timestamp, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	SourceInfo      string `json:"_source_info" parquet:"name=_source_info, type=BYTE_ARRAY, ConvertedType=UTF8"`
}

// DataFrame is a generic container for tabular data
type DataFrame[T any] struct {
	Records []T
	schema  interface{} // Used for schema inference
}

// CreateDataFrame creates a new DataFrame with the given records
func CreateDataFrame[T any](records []T) *DataFrame[T] {
	// Runtime pointer to the first record as schema reference
	var empty T
	schema := &empty

	return &DataFrame[T]{
		Records: records,
		schema:  schema,
	}
}

// ParquetWriterConfig holds configuration for Parquet writing
type ParquetWriterConfig struct {
	Compression parquet.CompressionCodec
	Concurrency int64
}

// DefaultParquetConfig returns the default configuration
func DefaultParquetConfig() ParquetWriterConfig {
	return ParquetWriterConfig{
		Compression: parquet.CompressionCodec_SNAPPY,
		Concurrency: 4,
	}
}

// WriteToParquet writes the DataFrame to a Parquet file using the provided writer
func (df *DataFrame[T]) WriteToParquet(fw source.ParquetFile, config ParquetWriterConfig) error {
	// Create the parquet writer
	pw, err := writer.NewParquetWriter(fw, df.schema, config.Concurrency)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression
	pw.CompressionType = config.Compression

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

// WriteToLocalParquet writes the DataFrame to a local Parquet file
func (df *DataFrame[T]) WriteToLocalParquet(filePath string, config ...ParquetWriterConfig) error {
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create local writer for path '%s': %w", filePath, err)
	}
	defer fw.Close()

	// Use provided config or default
	cfg := DefaultParquetConfig()
	if len(config) > 0 {
		cfg = config[0]
	}

	return df.WriteToParquet(fw, cfg)
}

type BaseSchemaParser[T any] struct{}

func (p *BaseSchemaParser[T]) ParseFromJson(
	rawData []byte,
	sourceInfo string,
) (T, error) {
	var record T

	// Parse the record data
	if err := json.Unmarshal(rawData, &record); err != nil {
		return record, fmt.Errorf("failed to parse record: %w", err)
	}

	// Calculate hash
	h := sha256.New()
	h.Write(rawData)
	recordInfo := RecordInfo{
		RawData:         string(rawData),
		SourceInfo:      sourceInfo,
		IngestTimestamp: int64(time.Now().UTC().UnixMilli()),
		RowHash:         hex.EncodeToString(h.Sum(nil)),
	}

	// Use reflection to set the RecordInfo field if it exists
	v := reflect.ValueOf(&record).Elem()
	f := v.FieldByName("RecordInfo")
	if f.IsValid() && f.CanSet() {

		f.Set(reflect.ValueOf(recordInfo))
	} else {
		return record, fmt.Errorf("type %T does not have a settable RecordInfo field", record)
	}

	return record, nil
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	Region          string
	Bucket          string
	Key             string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Endpoint        string // Optional for custom endpoints
}

// WriteToS3Parquet writes the DataFrame to an S3 Parquet file
func (df *DataFrame[T]) WriteToS3Parquet(ctx context.Context, bucket, key string, config ...ParquetWriterConfig) error {
	// Create S3 file writer
	fw, err := s3.NewS3FileWriter(ctx, bucket, key, nil)
	if err != nil {
		return fmt.Errorf("failed to create S3 writer for bucket '%s' and key '%s': %w",
			bucket, key, err)
	}
	defer fw.Close()

	// Use provided config or default
	cfg := DefaultParquetConfig()
	if len(config) > 0 {
		cfg = config[0]
	}

	return df.WriteToParquet(fw, cfg)
}

// ReadFromParquet reads a DataFrame from a Parquet file
func ReadFromParquet[T any](file source.ParquetFile) (*DataFrame[T], error) {
	// Create an empty instance for schema reference
	var empty T
	schema := &empty

	// Create parquet reader
	pr, err := reader.NewParquetReader(file, schema, 4) // Default concurrency of 4
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	// Get the number of rows
	numRows := int(pr.GetNumRows())

	// Prepare the slice to hold all records
	records := make([]T, numRows)

	// Read the data
	if err := pr.Read(&records); err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Create and return the DataFrame
	return CreateDataFrame(records), nil
}

// ReadFromLocalParquet reads a DataFrame from a local Parquet file
func ReadFromLocalParquet[T any](filePath string) (*DataFrame[T], error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file '%s': %w", filePath, err)
	}
	defer fr.Close()

	return ReadFromParquet[T](fr)
}

// ReadFromS3Parquet reads a DataFrame from an S3 Parquet file
func ReadFromS3Parquet[T any](ctx context.Context, bucket, key string) (*DataFrame[T], error) {
	fr, err := s3.NewS3FileReader(ctx, bucket, key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 parquet file at bucket '%s' key '%s': %w",
			bucket, key, err)
	}
	defer fr.Close()

	return ReadFromParquet[T](fr)
}

func main() {
	jsonData := `[
		{
			"Name": "Alice",
			"Age": 22,
			"Id": 1001,
			"Weight": 65.5,
			"Sex": false,
			"Day": 10957
		},
		{
			"Name": "Bob",
			"Age": 23,
			"Id": 1002,
			"Weight": 72.5,
			"Sex": true,
			"Day": 10731,
			"Ignored": 1
		}
	]`

	// Unmarshal the JSON array into a slice of json.RawMessage
	var rawRecords []json.RawMessage
	if err := json.Unmarshal([]byte(jsonData), &rawRecords); err != nil {
		fmt.Printf("failed to unmarshal JSON array: %v\n", err)
		os.Exit(1)
	}

	// Create a parser for the Student type.
	parser := BaseSchemaParser[Student]{}

	// Parse each raw record using ParseFromRaw
	var students []Student
	for _, raw := range rawRecords {
		student, err := parser.ParseFromJson(raw, "myjson")
		if err != nil {
			fmt.Printf("failed to parse record: %v\n", err)
			os.Exit(1)
		}
		students = append(students, student)
	}

	// Now students slice contains all enriched Student records.
	fmt.Printf("Parsed %d records\n", len(students))

	outputFilePath = "tmp/students.parquet"
	fmt.Printf("Creating student records and writing to: %s\n", outputFilePath)

	// Create DataFrame and write to Parquet
	df := CreateDataFrame(students)

	if err := df.WriteToLocalParquet(outputFilePath); err != nil {
		fmt.Printf("failed to write to parquet: %v\n", err)
		os.Exit(1)
	}

}
