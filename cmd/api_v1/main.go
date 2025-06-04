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
}

// Global variable to store users (bad practice, still populated from DB on start)
var users []User

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

	loadUsersFromDB()
}

// loadUsersFromDB loads users from the SQLite database into the global users slice
func loadUsersFromDB() {
	rows, err := db.Query("SELECT id, name, email FROM users")
	if err != nil {
		fmt.Println("Error querying users from DB:", err) // Just print
		return
	}
	defer rows.Close() // Defer but no error check on rows.Close()

	users = []User{} // Clear existing users before loading
	for rows.Next() {
		var u User
		err := rows.Scan(&u.ID, &u.Name, &u.Email)
		if err != nil {
			fmt.Println("Error scanning user row:", err) // Just print, skip problematic row
			continue
		}
		users = append(users, u)
	}

	if err := rows.Err(); err != nil {
		fmt.Println("Error iterating user rows:", err) // Just print
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

		// Still reads from the global, potentially racy 'users' slice
		// Linear search, inefficient for large N
		foundUser := false
		for _, user := range users {
			if user.ID == targetID {
				fmt.Fprintf(w, "User:\nID: %d, Name: %s, Email: %s\n", user.ID, user.Name, user.Email)
				foundUser = true
				break
			}
		}

		if !foundUser {
			http.Error(w, fmt.Sprintf("User with ID %d not found", targetID), http.StatusNotFound)
		}
		return
	}

	// If no ID parameter, return all users
	// Still reads from the global, potentially racy 'users' slice
	fmt.Fprintf(w, "Users:\n")
	for _, user := range users {
		fmt.Fprintf(w, "ID: %d, Name: %s, Email: %s\n", user.ID, user.Name, user.Email)
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

	// Simple ID generation (problematic in concurrent scenarios, and now potentially conflicting with DB PK)
	// This ID is based on the in-memory slice, which might be out of sync or racy.
	newID := len(users) + 1
	newUser := User{ID: newID, Name: name, Email: email}

	// Still append to the global 'users' slice (bad practice, racy)
	users = append(users, newUser)

	// Insert into SQLite database
	// The ID used here is the one generated from len(users), which is bad.
	// If 'id' in DB is AUTOINCREMENT, this explicit ID might cause issues or be overridden
	// depending on SQLite's behavior with PRIMARY KEY.
	// For this exercise, we'll attempt to insert with this potentially problematic ID.
	stmt, err := db.Prepare("INSERT INTO users(id, name, email) values(?,?,?)")
	if err != nil {
		fmt.Println("Error preparing insert statement:", err) // Just print
		// Note: The user was added to the in-memory 'users' slice but not to DB.
		// This maintains inconsistency, a "bad practice".
		http.Error(w, "Internal server error (DB prepare)", http.StatusInternalServerError) // Inform client somewhat
		return
	}
	// defer stmt.Close() // Good practice, but keeping it minimal like original

	_, err = stmt.Exec(newUser.ID, newUser.Name, newUser.Email)
	if err != nil {
		fmt.Println("Error executing insert statement:", err) // Just print
		// User is in memory 'users' slice but failed to save to DB.
		// We should ideally remove it from the 'users' slice here for consistency,
		// but to "keep bad Go usage", we'll leave it inconsistent.
		// The primary key constraint on ID might be violated here if newID conflicts.
		http.Error(w, "Internal server error (DB exec)", http.StatusInternalServerError) // Inform client somewhat
		// Attempt to remove the user from the in-memory slice if DB insert failed,
		// to reduce *some* inconsistency, though the ID generation is still flawed.
		// This is a slight deviation to prevent the in-memory slice from growing indefinitely on DB errors.
		if len(users) > 0 && users[len(users)-1].ID == newUser.ID { // Basic check
			users = users[:len(users)-1]
		}
		return
	}
	stmt.Close() // Close statement after execution

	fmt.Fprintf(w, "User added: ID %d, Name %s, Email %s\n", newUser.ID, newUser.Name, newUser.Email)
}

func main() {
	// Ensure db is closed when the application exits.
	// This is a minimal attempt at resource cleanup.
	// In a real app, signal handling for graceful shutdown is better.
	// defer func() {
	// 	if db != nil {
	// 		err := db.Close()
	// 		if err != nil {
	// 			fmt.Println("Error closing database:", err)
	// 		}
	// 	}
	// }() // This defer in main won't run if ListenAndServe blocks indefinitely or panics.

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
