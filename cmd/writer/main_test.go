package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// TestStudent is a simplified version of Student for testing
type TestStudent struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
}

func TestWriteToLocalParquet(t *testing.T) {
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
	df := CreateDataFrame(students)
	err := df.WriteToLocalParquet(tempFile)
	if err != nil {
		t.Fatalf("Failed to write to Parquet: %v", err)
	}

	// Read the Parquet file back to verify
	fr, err := local.NewLocalFileReader(tempFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(TestStudent), 4)
	if err != nil {
		t.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Check number of rows
	if num := pr.GetNumRows(); num != int64(len(students)) {
		t.Errorf("Expected %d rows, got %d", len(students), num)
	}

	// Read back the data
	readStudents := make([]TestStudent, len(students))
	if err := pr.Read(&readStudents); err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	// Verify the data
	for i, expected := range students {
		actual := readStudents[i]
		if expected.Name != actual.Name ||
			expected.Age != actual.Age ||
			expected.Id != actual.Id ||
			expected.Weight != actual.Weight {
			t.Errorf("Record %d mismatch: expected %+v, got %+v", i, expected, actual)
		}
	}
}
