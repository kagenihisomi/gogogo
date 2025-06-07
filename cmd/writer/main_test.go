package main

import (
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
