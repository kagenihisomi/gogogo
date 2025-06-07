package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	// Added proper import for MinIO credentials
)

// Happy path for the test file
func TestLocalParquet(t *testing.T) {
	type TestStudent struct {
		Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		Age    int32   `parquet:"name=age, type=INT32"`
		Id     int64   `parquet:"name=id, type=INT64"`
		Weight float32 `parquet:"name=weight, type=FLOAT"`
	}
	// Create test data
	students := []TestStudent{
		{Name: "Alice", Age: 20, Id: 1, Weight: 60.5},
		{Name: "Bob", Age: 22, Id: 2, Weight: 70.3},
		{Name: "Charlie", Age: 25, Id: 3, Weight: 80.1},
	}

	// Create directory if it doesn't exist
	dirPath := "tmp"
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a temporary file for testing
	tempFile := filepath.Join(dirPath, "test_students.parquet")
	defer os.Remove(tempFile) // Clean up after test

	// Create DataFrame and write to Parquet
	originalDF := CreateDataFrame(students)
	err := originalDF.WriteToLocalParquet(tempFile)
	if err != nil {
		t.Fatalf("Failed to write to Parquet: %v", err)
	}

	// Read the Parquet file back into a DataFrame
	readDF, err := ReadFromLocalParquet[TestStudent](tempFile)
	if err != nil {
		t.Fatalf("Failed to read from Parquet: %v", err)
	}

	// Compare the DataFrames
	if len(originalDF.Records) != len(readDF.Records) {
		t.Errorf("Record count mismatch: original=%d, read=%d",
			len(originalDF.Records), len(readDF.Records))
	}

	// Compare each record
	for i := 0; i < len(originalDF.Records); i++ {
		orig := originalDF.Records[i]
		read := readDF.Records[i]

		if orig.Name != read.Name {
			t.Errorf("Name mismatch at index %d: original=%s, read=%s", i, orig.Name, read.Name)
		}
		if orig.Age != read.Age {
			t.Errorf("Age mismatch at index %d: original=%d, read=%d", i, orig.Age, read.Age)
		}
		if orig.Id != read.Id {
			t.Errorf("Id mismatch at index %d: original=%d, read=%d", i, orig.Id, read.Id)
		}
		if orig.Weight != read.Weight {
			t.Errorf("Weight mismatch at index %d: original=%f, read=%f", i, orig.Weight, read.Weight)
		}
	}

	t.Logf("Successfully verified %d records", len(originalDF.Records))
}

// TestParseAndParquet tests the full pipeline: parsing JSON to Student structs with RecordInfo,
// writing to Parquet, reading back, and verifying all data remains intact.
func TestParseAndParquet(t *testing.T) {
	// Sample JSON data
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
		},
		{
			"Name": "Charlie",
			"Age": 25,
			"Id": 1003,
			"Weight": 68.3,
			"Sex": true,
			"Day": 11023
		}
	]`

	// Unmarshal the JSON array into a slice of json.RawMessage
	var rawRecords []json.RawMessage
	if err := json.Unmarshal([]byte(jsonData), &rawRecords); err != nil {
		t.Fatalf("Failed to unmarshal JSON array: %v", err)
	}

	// Create a parser for the Student type
	parser := BaseSchemaParser[Student]{}

	// Parse each raw record using ParseFromJson
	var students []Student
	sourceInfo := "test_source"
	for i, raw := range rawRecords {
		student, err := parser.ParseFromJson(raw, sourceInfo)
		if err != nil {
			t.Fatalf("Failed to parse record at index %d: %v", i, err)
		}
		students = append(students, student)
	}

	t.Logf("Parsed %d records with RecordInfo", len(students))

	// Create directory if it doesn't exist
	dirPath := "tmp"
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a temporary file for testing
	tempFile := filepath.Join(dirPath, "test_parsed_students.parquet")
	defer os.Remove(tempFile) // Clean up after test

	// Create DataFrame and write to Parquet
	originalDF := CreateDataFrame(students)
	err := originalDF.WriteToLocalParquet(tempFile)
	if err != nil {
		t.Fatalf("Failed to write to Parquet: %v", err)
	}

	// Read the Parquet file back into a DataFrame
	readDF, err := ReadFromLocalParquet[Student](tempFile)
	if err != nil {
		t.Fatalf("Failed to read from Parquet: %v", err)
	}

	// Compare the DataFrames
	if len(originalDF.Records) != len(readDF.Records) {
		t.Errorf("Record count mismatch: original=%d, read=%d",
			len(originalDF.Records), len(readDF.Records))
	}

	// Compare each record
	for i := 0; i < len(originalDF.Records); i++ {
		orig := originalDF.Records[i]
		read := readDF.Records[i]

		// Compare basic fields
		if orig.Name != read.Name {
			t.Errorf("Name mismatch at index %d: original=%s, read=%s", i, orig.Name, read.Name)
		}
		if orig.Age != read.Age {
			t.Errorf("Age mismatch at index %d: original=%d, read=%d", i, orig.Age, read.Age)
		}
		if orig.Id != read.Id {
			t.Errorf("Id mismatch at index %d: original=%d, read=%d", i, orig.Id, read.Id)
		}
		if orig.Weight != read.Weight {
			t.Errorf("Weight mismatch at index %d: original=%f, read=%f", i, orig.Weight, read.Weight)
		}
		if orig.Sex != read.Sex {
			t.Errorf("Sex mismatch at index %d: original=%t, read=%t", i, orig.Sex, read.Sex)
		}
		if orig.Day != read.Day {
			t.Errorf("Day mismatch at index %d: original=%d, read=%d", i, orig.Day, read.Day)
		}

		// Verify RecordInfo fields
		if orig.RecordInfo.RawData != read.RecordInfo.RawData {
			t.Errorf("RawData mismatch at index %d", i)
		}
		if orig.RecordInfo.RowHash != read.RecordInfo.RowHash {
			t.Errorf("RowHash mismatch at index %d", i)
		}
		if orig.RecordInfo.IngestTimestamp != read.RecordInfo.IngestTimestamp {
			t.Errorf("IngestTimestamp mismatch at index %d", i)
		}
		if orig.RecordInfo.SourceInfo != read.RecordInfo.SourceInfo {
			t.Errorf("SourceInfo mismatch at index %d", i)
		}
	}

	// Additional verification that RecordInfo was properly populated
	for i, student := range originalDF.Records {
		if student.RecordInfo.SourceInfo != sourceInfo {
			t.Errorf("SourceInfo not set correctly at index %d", i)
		}
		if student.RecordInfo.RowHash == "" {
			t.Errorf("RowHash not generated at index %d", i)
		}
		if student.RecordInfo.IngestTimestamp == 0 {
			t.Errorf("IngestTimestamp not set at index %d", i)
		}
		if student.RecordInfo.RawData == "" {
			t.Errorf("RawData not captured at index %d", i)
		}
	}

	t.Logf("Successfully verified %d records with RecordInfo", len(originalDF.Records))
}
