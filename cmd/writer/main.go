package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
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
	// Added field for record-level ETL metadata
	RecordInfo `parquet:"name=_recordinfo, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
}

type RecordInfo struct {
	RawData         string `json:"_raw_data" parquet:"name=_raw_data, type=BYTE_ARRAY, ConvertedType=UTF8"`
	RowHash         string `json:"_row_hash" parquet:"name=_row_hash, type=BYTE_ARRAY, ConvertedType=UTF8"`
	IngestTimestamp int64  `json:"_ingest_timestamp" parquet:"name=_ingest_timestamp, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	SourceSystem    string `json:"_source_system" parquet:"name=_source_system, type=BYTE_ARRAY, ConvertedType=UTF8"`
	SourceEndpoint  string `json:"_source_endpoint" parquet:"name=_source_endpoint, type=BYTE_ARRAY, ConvertedType=UTF8"`
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

// WriteToLocalParquet writes the DataFrame to a local Parquet file
func (df *DataFrame[T]) WriteToLocalParquet(filePath string) error {
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

type BaseSchemaParser[T any] struct{}

func (p *BaseSchemaParser[T]) ParseFromRaw(
	rawData []byte,
	sourceSystem,
	sourceEndpoint string,
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
		SourceSystem:    sourceSystem,
		SourceEndpoint:  sourceEndpoint,
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
            "Day": 10731
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
		student, err := parser.ParseFromRaw(raw, "mysource", "myendpoint")
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

	df.WriteToLocalParquet(outputFilePath)

}
