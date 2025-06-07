package datarizer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3" // Use alias to avoid conflict
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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

// TestLocalJSONL tests writing to and reading from a local JSONL file
func TestLocalJSONL(t *testing.T) {
	type TestStudent struct {
		Name   string  `json:"name"`
		Age    int32   `json:"age"`
		Id     int64   `json:"id"`
		Weight float32 `json:"weight"`
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
	tempFile := filepath.Join(dirPath, "test_students.jsonl")
	defer os.Remove(tempFile) // Clean up after test

	// Create DataFrame and write to JSONL
	originalDF := CreateDataFrame(students)
	err := originalDF.WriteToJSONL(tempFile)
	if err != nil {
		t.Fatalf("Failed to write to JSONL: %v", err)
	}

	// Read the JSONL file back into a DataFrame
	readDF, err := ReadFromJSONL[TestStudent](tempFile)
	if err != nil {
		t.Fatalf("Failed to read from JSONL: %v", err)
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

// TestS3Parquet tests writing to and reading from an S3-compatible storage (MinIO)
func TestS3Parquet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping S3 test in short mode")
	}

	// Setup MinIO
	bucketName, _, s3Client, cleanup := setupMinioS3(t)
	defer cleanup()

	// Setup test data
	ctx := context.Background()
	keyName := "test-data/students.parquet"
	// Define a function-scoped test type
	type TestStudent struct {
		Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		Age    int32   `parquet:"name=age, type=INT32"`
		Id     int64   `parquet:"name=id, type=INT64"`
		Weight float32 `parquet:"name=weight, type=FLOAT"`
		Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
		Day    int32   `parquet:"name=day, type=INT32"`
	}

	// Prepare test data
	students := []TestStudent{
		{Name: "Alice", Age: 20, Id: 1001, Weight: 60.5, Sex: false, Day: 10957},
		{Name: "Bob", Age: 22, Id: 1002, Weight: 70.3, Sex: true, Day: 10731},
	}

	// Write to S3 using the existing function
	df := CreateDataFrame(students)
	err := df.WriteToS3Parquet(ctx, s3Client, bucketName, keyName)
	if err != nil {
		t.Fatalf("Failed to write to S3: %v", err)
	}

	// List objects in bucket for debugging
	listResult, err := s3Client.ListObjectsV2(&awsS3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Could not list objects: %v", err)
	} else {
		t.Logf("Objects in bucket:")
		for _, obj := range listResult.Contents {
			t.Logf("  - %s", *obj.Key)
		}
	}

	// Verify the file exists before reading
	_, err = s3Client.HeadObject(&awsS3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
	})
	if err != nil {
		t.Fatalf("File was not written or not accessible: %v", err)
	}

	// Read from S3 using the existing function
	readDF, err := ReadFromS3Parquet[TestStudent](ctx, s3Client, bucketName, keyName)
	if err != nil {
		t.Fatalf("Failed to read from S3: %v", err)
	}

	// Verify data
	if len(readDF.Records) != len(students) {
		t.Errorf("Record count mismatch: expected=%d, got=%d",
			len(students), len(readDF.Records))
	}

	for i, student := range students {
		read := readDF.Records[i]
		if student.Name != read.Name || student.Age != read.Age || student.Id != read.Id {
			t.Errorf("Record %d data mismatch", i)
		}
		if student.Sex != read.Sex || student.Day != read.Day || student.Weight != read.Weight {
			t.Errorf("Record %d extended data mismatch", i)
		}
	}

	t.Logf("Successfully verified %d records from S3", len(readDF.Records))
}

// setupMinioS3 creates a MinIO container and configures it for testing
// Returns: bucketName, minioURL, s3Client, cleanup function
func setupMinioS3(t *testing.T) (string, string, *awsS3.S3, func()) {
	// Setup Docker
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to Docker: %v", err)
	}

	// Start MinIO container
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Env: []string{
			"MINIO_ROOT_USER=minioadmin",
			"MINIO_ROOT_PASSWORD=minioadmin",
		},
		Cmd: []string{"server", "/data"},
		ExposedPorts: []string{
			"9000/tcp",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		t.Fatalf("Could not start MinIO container: %v", err)
	}

	// Get the container's host and port
	minioPort := minioResource.GetPort("9000/tcp")
	minioEndpoint := fmt.Sprintf("localhost:%s", minioPort)
	minioURL := fmt.Sprintf("http://%s", minioEndpoint)

	// Wait for MinIO to be ready
	if err := pool.Retry(func() error {
		s3Config := &aws.Config{
			Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
			Endpoint:         aws.String(minioURL),
			Region:           aws.String("us-east-1"),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		}
		s3Session, err := session.NewSession(s3Config)
		if err != nil {
			return err
		}
		s3Client := awsS3.New(s3Session)

		// Try to list buckets to see if MinIO is responding
		_, err = s3Client.ListBuckets(nil)
		return err
	}); err != nil {
		if purgeErr := pool.Purge(minioResource); purgeErr != nil {
			t.Logf("Warning: Failed to purge MinIO container: %v", purgeErr)
		}
		t.Fatalf("Could not connect to MinIO: %v", err)
	}

	// Create S3 client for testing
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		Endpoint:         aws.String(minioURL),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	s3Session, err := session.NewSession(s3Config)
	if err != nil {
		if purgeErr := pool.Purge(minioResource); purgeErr != nil {
			t.Logf("Warning: Failed to purge MinIO container: %v", purgeErr)
		}
		t.Fatalf("Could not create S3 session: %v", err)
	}
	s3Client := awsS3.New(s3Session)

	// Create bucket
	bucketName := "test-bucket"
	_, err = s3Client.CreateBucket(&awsS3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		if purgeErr := pool.Purge(minioResource); purgeErr != nil {
			t.Logf("Warning: Failed to purge MinIO container: %v", purgeErr)
		}
		t.Fatalf("Could not create bucket: %v", err)
	}

	// Add a policy to allow all operations
	policy := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:*"],
            "Resource": ["arn:aws:s3:::test-bucket", "arn:aws:s3:::test-bucket/*"]
        }
    ]
}`

	_, err = s3Client.PutBucketPolicy(&awsS3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(policy),
	})
	if err != nil {
		if purgeErr := pool.Purge(minioResource); purgeErr != nil {
			t.Logf("Warning: Failed to purge MinIO container: %v", purgeErr)
		}
		t.Fatalf("Could not set bucket policy: %v", err)
	}

	// Save current environment variables
	originalEndpoint := os.Getenv("AWS_ENDPOINT")
	originalRegion := os.Getenv("AWS_REGION")
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	originalForcePathStyle := os.Getenv("AWS_S3_FORCE_PATH_STYLE")
	originalSDKLoadConfig := os.Getenv("AWS_SDK_LOAD_CONFIG")
	originalAllowHTTP := os.Getenv("AWS_ALLOW_HTTP")

	// Set environment for test
	os.Setenv("AWS_ENDPOINT", minioURL)
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	os.Setenv("AWS_S3_FORCE_PATH_STYLE", "true")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	os.Setenv("AWS_ALLOW_HTTP", "true") // Critical for local MinIO testing

	// Return cleanup function
	cleanup := func() {
		// Restore original environment variables
		os.Setenv("AWS_ENDPOINT", originalEndpoint)
		os.Setenv("AWS_REGION", originalRegion)
		os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
		os.Setenv("AWS_S3_FORCE_PATH_STYLE", originalForcePathStyle)
		os.Setenv("AWS_SDK_LOAD_CONFIG", originalSDKLoadConfig)
		os.Setenv("AWS_ALLOW_HTTP", originalAllowHTTP)

		// Clean up the container
		if err := pool.Purge(minioResource); err != nil {
			t.Logf("Could not purge MinIO container: %v", err)
		}
	}
	// Verify basic S3 functionality
	verifyS3Functionality(t, s3Client, bucketName)

	return bucketName, minioURL, s3Client, cleanup
}

// verifyS3Functionality uploads a simple test file to verify basic S3 functionality
func verifyS3Functionality(t *testing.T, s3Client *awsS3.S3, bucket string) {
	testContent := []byte("test content")
	testKey := "test-file.txt"

	// Upload a simple file
	_, err := s3Client.PutObject(&awsS3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(testKey),
		Body:        bytes.NewReader(testContent),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("Failed to upload test file: %v", err)
	}

	// Verify the test file exists
	_, err = s3Client.HeadObject(&awsS3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
	})
	if err != nil {
		t.Fatalf("Test file was not accessible: %v", err)
	}
	t.Logf("Basic S3 functionality verified")
}
