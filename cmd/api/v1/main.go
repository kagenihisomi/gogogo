package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv" // Added for Atoi

	// Keep for os.Exit or other non-file uses if any; not strictly needed for this refactor
	// Keep for Atoi if it were used elsewhere; not strictly needed for this refactor
	// Keep for strings.Split if it were used elsewhere; not strictly needed for this refactor
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// User struct
type User struct {
	ID    int
	Name  string
	Email string
	Age   int `json:"age"` // Added Age field to User struct
}

// dbFileName where SQLite data is stored
const dbFileName = "users.db"

// Global database connection pool (bad practice to not manage its lifecycle carefully, but kept)
var db *sql.DB

// init function to load data on startup (can be problematic)
func init() {
	var err error
	db, err = sql.Open("sqlite3", dbFileName)
	if err != nil {
		fmt.Println("Error opening database:", err) // Just print, no proper handling
		// In a real app, you'd likely os.Exit(1) or panic here if DB is critical
		return
	}
	// db.Close() should be called on shutdown, but we're keeping bad practices

	createTableSQL := `CREATE TABLE IF NOT EXISTS users (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"name" TEXT,
		"email" TEXT,
		"age" INTEGER DEFAULT 0 -- Added age column with default value
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err) // Log fatal, but still not a good practice
		return
	}
}

// Handlers for HTTP requests
func handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	idParam := query.Get("id")

	if idParam != "" {
		// Attempt to find a single user by ID
		targetID, err := strconv.Atoi(idParam)
		if err != nil {
			http.Error(w, "Invalid user ID format", http.StatusBadRequest)
			return
		}

		dbRows, err := db.Query("SELECT id, name, email, age FROM users WHERE id = ?", targetID)
		if err != nil {
			http.Error(w, "Internal server error (DB query)", http.StatusInternalServerError)
			return
		}
		defer dbRows.Close() // Ensure rows are closed

		foundUser := false
		for dbRows.Next() {
			foundUser = true
			var user User
			err := dbRows.Scan(&user.ID, &user.Name, &user.Email, &user.Age)
			if err != nil {
				http.Error(w, "Internal server error (DB scan)", http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "ID: %d, Name: %s, Email: %s\n", user.ID, user.Name, user.Email)
		}

		if !foundUser {
			http.Error(w, fmt.Sprintf("User with ID %d not found", targetID), http.StatusNotFound)
		}
		return
	}
}

func handleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	name := query.Get("name")
	email := query.Get("email")

	// Validate name and email parameters
	if name == "" || email == "" {
		http.Error(w, "Name and Email are required", http.StatusBadRequest)
		return
	}
	if len(query) != 2 {
		http.Error(w, "Invalid parameters: only name and email are allowed.", http.StatusBadRequest)
		return
	}

	// Insert into SQLite database, letting the DB generate the ID
	stmt, err := db.Prepare("INSERT INTO users(name, email) values(?,?)")
	if err != nil {
		fmt.Println("Error preparing insert statement:", err)
		http.Error(w, "Internal server error (DB prepare)", http.StatusInternalServerError)
		return
	}
	defer stmt.Close() // Ensure statement is closed

	_, err = stmt.Exec(name, email)
	if err != nil {
		fmt.Println("Error executing insert statement:", err)
		http.Error(w, "Internal server error (DB exec)", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "User added: Name %s, Email %s\n", name, email)
}

func main() {
	http.HandleFunc("/users", handleGetUsers)
	http.HandleFunc("/add", handleAddUser)

	fmt.Println("Server starting on :8080, using SQLite backend.")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Server failed:", err) // No proper error handling
		if db != nil {                     // Attempt to close DB if server fails to start
			db.Close()
		}
	}

}
