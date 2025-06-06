package main

import (
	"context"
	"fmt"
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
	// Skip this test on Windows
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test on Windows: rootless Docker not supported")
	}

	ctx := context.Background()

	// Set up MinIO container
	container, endpoint, err := setupMinioContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to set up MinIO: %v", err)
	}
	defer container.Terminate(ctx)

	// Create MinIO client
	minioClient, err := createMinioClient(endpoint)
	if err != nil {
		t.Fatalf("Failed to create MinIO client: %v", err)
	}

	// Ensure test bucket exists
	bucket := "test-bucket"
	if err := ensureBucketExists(ctx, minioClient, bucket); err != nil {
		t.Fatalf("Failed to ensure bucket exists: %v", err)
	}

	// Prepare test data
	students := []Student{
		{
			Name:   "Test Student",
			Age:    30,
			Id:     1,
			Weight: 70.0,
			Sex:    true,
			Day:    1,
		},
	}
	df := CreateDataFrame(students)

	// Write data to S3 (with context)
	key := "students.parquet"
	if err := df.WriteToS3Parquet(ctx, bucket, key); err != nil {
		t.Fatalf("Failed to write to S3: %v", err)
	}

	// Verify file was written correctly
	objInfo, err := minioClient.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to stat object: %v", err)
	}
	if objInfo.Size == 0 {
		t.Fatalf("Object size is zero, file may be empty")
	}

	t.Logf("Successfully wrote and verified Parquet file of size %d bytes", objInfo.Size)
}
