package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/xitongsys/parquet-go-source/local" //
	"github.com/xitongsys/parquet-go/parquet"      // Added for compression codecs
	"github.com/xitongsys/parquet-go/writer"       // For simpler Parquet writing
)

// User struct to match the FastAPI UserResponse
type User struct {
	ID    int    `json:"id" parquet:"name=id, type=INT32"`
	Name  string `json:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Email string `json:"email" parquet:"name=email, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age   int    `json:"age" parquet:"name=age, type=INT32"` // FastAPI defaults age to 0, so it should always be present
}

const (
	baseURL          = "http://localhost:8000/users/"
	defaultPageLimit = 50 // Number of users to request per page (FastAPI max is 100)
	maxRetries       = 5
	initialBackoff   = 1 * time.Second
	maxBackoff       = 30 * time.Second
	requestTimeout   = 15 * time.Second // Timeout for each individual HTTP request attempt
	totalJobTimeout  = 5 * time.Minute  // Optional: A total timeout for the entire ETL job
)

// sharedRetryableClient is a shared client for connection reuse and retries.
var sharedRetryableClient *retryablehttp.Client

func init() {
	// Seed the global random number generator (good practice, though retryablehttp handles its own jitter)
	rand.Seed(time.Now().UnixNano())

	client := retryablehttp.NewClient()
	client.RetryMax = maxRetries
	client.RetryWaitMin = initialBackoff
	client.RetryWaitMax = maxBackoff
	// The client.HTTPClient is a standard *http.Client.
	// We set its timeout for individual attempts made by the retryablehttp client.
	client.HTTPClient.Timeout = requestTimeout

	// Configure the logger for retryablehttp.
	// Set to nil or a logger that writes to io.Discard to suppress verbose logging from the library.
	// If you need to debug retry attempts, you can set it to log.Default() or a custom logger.
	client.Logger = nil // Suppress verbose library logging by default

	// The DefaultRetryPolicy is generally sufficient and covers common retry scenarios
	// like network errors, 429s, and 5xx server errors.
	// client.CheckRetry = retryablehttp.DefaultRetryPolicy (this is the default)

	sharedRetryableClient = client
}

func main() {
	log.Println("Starting ETL process to fetch all users...")

	// Overall context for the entire ETL job
	ctx, cancelJob := context.WithTimeout(context.Background(), totalJobTimeout)
	defer cancelJob()

	allUsers, err := fetchAllUsers(ctx)
	if err != nil {
		log.Fatalf("ETL process failed: %v", err)
	}

	log.Printf("Successfully fetched %d users.\n", len(allUsers))

	// Example: Writing to JSON
	jsonFilePath := "tmp/users.json"
	if err := writeUsersToJSON(allUsers, jsonFilePath); err != nil {
		log.Fatalf("Failed to write users to JSON: %v", err)
	}
	log.Printf("Successfully wrote users to %s\n", jsonFilePath)

	// Example: Writing to Parquet using xitongsys/parquet-go
	parquetSimpleFilePath := "tmp/users_simple.parquet"
	if err := writeUsersToParquetSimple(allUsers, parquetSimpleFilePath); err != nil {
		log.Fatalf("Failed to write users to Parquet (simple): %v", err)
	}
	log.Printf("Successfully wrote users to %s\n", parquetSimpleFilePath)
}

// fetchAllUsers handles the pagination logic to retrieve all users.
func fetchAllUsers(ctx context.Context) ([]User, error) {
	var allUsers []User
	skip := 0
	limit := defaultPageLimit

	for {
		// Check for overall job cancellation before fetching a page
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("job cancelled or timed out: %w", ctx.Err())
		default:
		}

		log.Printf("Fetching page: skip=%d, limit=%d\n", skip, limit)
		pageUsers, err := fetchPageWithRetryableClient(ctx, baseURL, skip, limit)
		if err != nil {
			return nil, fmt.Errorf("error fetching page at skip %d: %w", skip, err)
		}

		if len(pageUsers) == 0 {
			log.Println("Received empty page, assuming end of data.")
			break // No more users
		}

		allUsers = append(allUsers, pageUsers...)

		if len(pageUsers) < limit {
			log.Printf("Received %d users, which is less than limit %d. Assuming end of data.", len(pageUsers), limit)
			break // This was the last page
		}

		skip += limit // Move to the next page
	}
	return allUsers, nil
}

// fetchPageWithRetryableClient attempts to fetch a single page of users
// using the configured retryablehttp.Client.
func fetchPageWithRetryableClient(ctx context.Context, targetURL string, skip int, limit int) ([]User, error) {
	// Construct URL with query parameters
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL '%s': %w", targetURL, err)
	}
	queryParams := parsedURL.Query()
	queryParams.Set("skip", strconv.Itoa(skip))
	queryParams.Set("limit", strconv.Itoa(limit))
	parsedURL.RawQuery = queryParams.Encode()
	fullURL := parsedURL.String()

	// The context passed to NewRequestWithContext governs the entire Do operation,
	// including all retries and backoff periods.
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		// This error is critical (e.g., bad method for NewRequestWithContext)
		return nil, fmt.Errorf("failed to create HTTP request for %s: %w", fullURL, err)
	}
	req.Header.Set("Accept", "application/json")

	log.Printf("Sending GET request (via retryable client) to %s\n", fullURL)
	resp, err := sharedRetryableClient.Do(req)
	if err != nil {
		// This error means all retries by sharedRetryableClient have been exhausted,
		// or a non-retryable error occurred as per its CheckRetry policy,
		// or the parent context (ctx) was cancelled.
		return nil, fmt.Errorf("failed to fetch page from %s after retries: %w", fullURL, err)
	}
	defer resp.Body.Close()

	// Read body (after successful response from retryablehttp client)
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		// This error occurs after a response was successfully received (headers).
		// retryablehttp won't retry this part.
		return nil, fmt.Errorf("failed to read response body from %s (status %d): %w", fullURL, resp.StatusCode, readErr)
	}

	// Check status code. retryablehttp.Do returns an error for non-2xx responses
	// that are not retried further. However, it's good practice to verify.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned non-OK status %d for %s after retries. Body: %s", resp.StatusCode, fullURL, string(body))
	}

	var users []User
	if err := json.Unmarshal(body, &users); err != nil {
		// JSON unmarshalling error after a 200 OK.
		// This is treated as a terminal error for this page fetch.
		return nil, fmt.Errorf("failed to unmarshal JSON response from %s (status %d). Body: %s. Error: %w",
			fullURL, resp.StatusCode, string(body), err)
	}

	return users, nil
}

// writeUsersToJSON writes a slice of User structs to a JSON file.
func writeUsersToJSON(users []User, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create JSON file '%s': %w", filePath, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Optional: for pretty printing
	if err := encoder.Encode(users); err != nil {
		return fmt.Errorf("failed to encode users to JSON file '%s': %w", filePath, err)
	}
	log.Printf("Successfully wrote %d users to JSON file: %s\n", len(users), filePath)
	return nil
}

// writeUsersToParquetSimple writes a slice of User structs to a Parquet file
// using the xitongsys/parquet-go library.
func writeUsersToParquetSimple(users []User, filePath string) error {
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create local file writer for parquet '%s': %w", filePath, err)
	}
	defer fw.Close()

	// new(User) is used for schema inference.
	// The third argument is the concurrency for writing, 1 is fine for this case.
	pw, err := writer.NewParquetWriter(fw, new(User), 1)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// You can customize writer properties if needed, e.g., compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY // Example

	for _, user := range users {
		if err := pw.Write(user); err != nil {
			// Attempt to stop writer to clean up, but prioritize the write error
			_ = pw.WriteStop() // Best effort to close
			return fmt.Errorf("failed to write user record (ID: %d) to parquet: %w", user.ID, err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop parquet writer: %w", err)
	}
	log.Printf("Successfully wrote %d users to Parquet file (simple): %s\n", len(users), filePath)
	return nil
}
