package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupMinioContainer creates and starts a MinIO container for testing
func setupMinioContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "minio/minio",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioaccesskey",
			"MINIO_ROOT_PASSWORD": "miniosecretkey",
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to create MinIO container: %w", err)
	}

	// Get endpoint details
	host, err := container.Host(ctx)
	if err != nil {
		return container, "", fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return container, "", fmt.Errorf("failed to get mapped port: %w", err)
	}

	endpoint := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	return container, endpoint, nil
}

// createMinioClient creates a MinIO client connected to the specified endpoint
func createMinioClient(endpoint string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioaccesskey", "miniosecretkey", ""),
		Secure: false,
		Region: "us-east-1",
	})
}

// ensureBucketExists creates a bucket if it doesn't already exist
func ensureBucketExists(ctx context.Context, client *minio.Client, bucket string) error {
	err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"})
	if err != nil {
		// Check if bucket already exists
		exists, errExists := client.BucketExists(ctx, bucket)
		if errExists != nil || !exists {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return nil
}

func TestWriteToS3Parquet(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test on Windows: rootless Docker not supported")
	}

	// Setup test environment
	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-students.parquet"
	endpoint := "localhost:9000"

	// Save original environment variables
	origAWSAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	origAWSSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	origAWSEndpoint := os.Getenv("AWS_ENDPOINT")
	origAWSRegion := os.Getenv("AWS_REGION")

	// Set environment variables for test
	os.Setenv("AWS_ACCESS_KEY_ID", "minioaccesskey")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "miniosecretkey")
	os.Setenv("AWS_ENDPOINT", endpoint)
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("AWS_S3_FORCE_PATH_STYLE", "true") // Important for MinIO

	// Restore environment after test
	defer func() {
		os.Setenv("AWS_ACCESS_KEY_ID", origAWSAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", origAWSSecretKey)
		os.Setenv("AWS_ENDPOINT", origAWSEndpoint)
		os.Setenv("AWS_REGION", origAWSRegion)
		os.Unsetenv("AWS_S3_FORCE_PATH_STYLE")
	}()

	// Start MinIO container
	minioC, err := startMinioContainer(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start MinIO container: %v", err)
	}
	defer minioC.Terminate(ctx)

	// Create MinIO client using same credentials as environment
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			""),
		Secure: false,
		Region: os.Getenv("AWS_REGION"),
	})
	if err != nil {
		t.Fatalf("Failed to create MinIO client: %v", err)
	}

	// Create bucket
	if err := minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create test data
	students := []TestStudent{
		{Name: "Alice", Age: 20, Id: 1, Weight: 60.5},
		{Name: "Bob", Age: 22, Id: 2, Weight: 70.3},
	}

	// Create DataFrame
	df := CreateDataFrame(students)

	// Write to S3 using the original interface
	// The AWS SDK will pick up credentials from environment variables
	if err := df.WriteToS3Parquet(ctx, bucket, key); err != nil {
		t.Fatalf("Failed to write to S3: %v", err)
	}

	// Verify the object exists
	info, err := minioClient.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to stat object: %v", err)
	}

	t.Logf("Successfully wrote and verified object: %s/%s (size: %d bytes)",
		bucket, key, info.Size)

	// Additional verification could download and parse the Parquet file
}
