package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/kagenihisomi/datarizer/datarizer"
)

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

	// Create a parser for the Student type
	parser := datarizer.BaseSchemaParser[datarizer.Student]{}

	// Parse each raw record using ParseFromJson
	var students []datarizer.Student
	for _, raw := range rawRecords {
		student, err := parser.ParseFromJson(raw, "myjson")
		if err != nil {
			fmt.Printf("failed to parse record: %v\n", err)
			os.Exit(1)
		}
		students = append(students, student)
	}

	// Now students slice contains all enriched Student records
	fmt.Printf("Parsed %d records\n", len(students))

	// Create DataFrame and write to Parquet
	df := datarizer.CreateDataFrame(students)

	if err := df.WriteToJSONL("tmp/students.jsonl"); err != nil {
		fmt.Printf("failed to write to JSONL: %v\n", err)
		os.Exit(1)
	}

	if err := df.WriteToLocalParquet("tmp/students.parquet"); err != nil {
		fmt.Printf("failed to write to parquet: %v\n", err)
		os.Exit(1)
	}
}
