package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
	// This handler is now part of usersHandlerFunc in main.go,
	// but for isolated unit testing, we can still test handleAddUser directly.
	// If you were testing the mux, you'd set up the mux.
	addUserHandler := handleAddUser(testDB) // Assuming handleAddUser is still accessible for testing

	t.Run("Positive case - add user successfully", func(t *testing.T) {
		userData := User{Name: "TestUser", Email: "test@example.com", Age: 30}
		payload, _ := json.Marshal(userData)
		req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		addUserHandler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusCreated {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusCreated, rr.Body.String())
		}

		var createdUser User
		err = json.NewDecoder(rr.Body).Decode(&createdUser)
		if err != nil {
			t.Fatalf("Could not decode response body: %v", err)
		}

		if createdUser.Name != userData.Name || createdUser.Email != userData.Email || createdUser.Age != userData.Age {
			t.Errorf("handler returned unexpected body: got %+v want name=%s, email=%s, age=%d", createdUser, userData.Name, userData.Email, userData.Age)
		}
		if createdUser.ID == 0 {
			t.Errorf("Expected created user to have an ID, got %d", createdUser.ID)
		}

		// Verify in DB
		var name string
		var age int
		err = testDB.QueryRow("SELECT name, age FROM users WHERE email = ?", "test@example.com").Scan(&name, &age)
		if err != nil {
			t.Fatalf("Failed to query test DB: %v", err)
		}
		if name != "TestUser" {
			t.Errorf("Expected name 'TestUser' in DB, got '%s'", name)
		}
		if age != 30 {
			t.Errorf("Expected age 30 in DB, got '%d'", age)
		}
	})

	t.Run("Negative case - missing name", func(t *testing.T) {
		userData := map[string]interface{}{"email": "onlyemail@example.com", "age": 25} // Name is missing
		payload, _ := json.Marshal(userData)
		req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		addUserHandler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusBadRequest, rr.Body.String())
		}
		expectedBody := "Name and Email are required\n" // main.go uses http.Error which appends a newline
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})

	t.Run("Negative case - invalid JSON payload (e.g. age as string)", func(t *testing.T) {
		payload := []byte(`{"name": "BadAge", "email": "badage@example.com", "age": "thirty"}`)
		req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		addUserHandler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusBadRequest, rr.Body.String())
		}
		// The exact error message from json.Decode can be a bit verbose or change slightly.
		// Checking for a key part of it is often sufficient.
		if !strings.Contains(rr.Body.String(), "Invalid request payload") {
			t.Errorf("handler returned unexpected body: got %q, expected to contain 'Invalid request payload'", rr.Body.String())
		}
	})

	t.Run("Negative case - empty JSON payload", func(t *testing.T) {
		payload := []byte(`{}`)
		req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		addUserHandler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusBadRequest, rr.Body.String())
		}
		expectedBody := "Name and Email are required\n"
		if rr.Body.String() != expectedBody {
			t.Errorf("handler returned unexpected body: got %q want %q", rr.Body.String(), expectedBody)
		}
	})

	t.Run("Negative case - malformed JSON", func(t *testing.T) {
		payload := []byte(`{"name": "Malformed", "email": "malformed@example.com", "age": 30,`) // Missing closing brace
		req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		addUserHandler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v. Body: %s", status, http.StatusBadRequest, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "Invalid request payload") {
			t.Errorf("handler returned unexpected body: got %q, expected to contain 'Invalid request payload'", rr.Body.String())
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
