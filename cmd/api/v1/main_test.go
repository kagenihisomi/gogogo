package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// setupTestDB initializes an in-memory SQLite database for testing.
// It returns the test database connection and a cleanup function.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	testDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	// Re-create table schema for the test database, same as in newDB
	createTableSQL := `CREATE TABLE IF NOT EXISTS users (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"name" TEXT,
		"email" TEXT,
		"age" INTEGER DEFAULT 0
	);`
	_, err = testDB.Exec(createTableSQL)
	if err != nil {
		testDB.Close()
		t.Fatalf("Failed to create table in test database: %v", err)
	}

	return testDB, func() {
		testDB.Close()
	}
}

// TestHandleAddUser tests the handleAddUser handler.
func TestHandleAddUser(t *testing.T) {
	testDB, cleanup := setupTestDB(t)
	defer cleanup()

	// Get the handler by calling handleAddUser with the testDB
	handler := handleAddUser(testDB)

	t.Run("Positive case - add user successfully", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/add?name=TestUser&email=test@example.com&age=30", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusOK, rr.Body.String())
		}

		body := rr.Body.String()
		if !strings.Contains(body, "User added:") || !strings.Contains(body, "Name TestUser") || !strings.Contains(body, "Email test@example.com") || !strings.Contains(body, "Age 30") {
			t.Errorf("handler returned unexpected body: got %q", body)
		}

		// Verify in DB
		var name string
		err = testDB.QueryRow("SELECT name FROM users WHERE email = ?", "test@example.com").Scan(&name)
		if err != nil {
			t.Fatalf("Failed to query test DB: %v", err)
		}
		if name != "TestUser" {
			t.Errorf("Expected name 'TestUser' in DB, got '%s'", name)
		}
	})

	t.Run("Negative case - missing name", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/add?email=onlyemail@example.com&age=25", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusBadRequest)
		}
		expectedBody := "Name and Email are required\n"
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})

	t.Run("Negative case - invalid age format", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/add?name=BadAge&email=badage@example.com&age=thirty", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusBadRequest)
		}
		expectedBody := "Invalid age format\n"
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})
}

// TestHandleGetUsers tests the handleGetUsers handler.
func TestHandleGetUsers(t *testing.T) {
	testDB, cleanup := setupTestDB(t)
	defer cleanup()

	// Get the handler by calling handleGetUsers with the testDB
	handler := handleGetUsers(testDB)

	// Pre-populate data
	_, err := testDB.Exec("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 28)")
	if err != nil {
		t.Fatalf("DB insert failed: %v", err)
	}
	var bobID int64
	res, err := testDB.Exec("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 32)")
	if err != nil {
		t.Fatalf("DB insert failed: %v", err)
	}
	bobID, _ = res.LastInsertId()

	t.Run("Positive case - get all users", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/users", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "Name: Alice") || !strings.Contains(body, "Age: 28") {
			t.Errorf("Response missing Alice's data: %s", body)
		}
		if !strings.Contains(body, "Name: Bob") || !strings.Contains(body, "Age: 32") {
			t.Errorf("Response missing Bob's data: %s", body)
		}
	})

	t.Run("Positive case - get specific user by ID", func(t *testing.T) {
		req, err := http.NewRequest("GET", fmt.Sprintf("/users?id=%d", bobID), nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}
		expectedBodyPart := fmt.Sprintf("ID: %d, Name: Bob, Email: bob@example.com, Age: 32", bobID)
		if !strings.Contains(rr.Body.String(), expectedBodyPart) {
			t.Errorf("handler returned unexpected body: got %q, want to contain %q", rr.Body.String(), expectedBodyPart)
		}
	})

	t.Run("Negative case - get specific user, ID not found", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/users?id=9999", nil) // Non-existent ID
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusNotFound {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusNotFound)
		}
		expectedBody := "User with ID 9999 not found\n"
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})

	t.Run("Negative case - get specific user, invalid ID format", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/users?id=abc", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusBadRequest)
		}
		expectedBody := "Invalid user ID format\n"
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})
}
