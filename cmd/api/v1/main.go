package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

// User struct
type User struct {
	ID    int
	Name  string
	Email string
	Age   int `json:"age"`
}

// dbFileName where SQLite data is stored
const dbFileName = "users.db"

// newDB initializes the database connection and creates the table if it doesn't exist.
// It returns the database connection pool or an error.
func newDB(dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// Ping the database to verify the connection.
	if err = db.Ping(); err != nil {
		db.Close() // Close the connection if ping fails
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	createTableSQL := `CREATE TABLE IF NOT EXISTS users (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "name" TEXT,
        "email" TEXT,
        "age" INTEGER DEFAULT 0
    );`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close() // Close the connection if table creation fails
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return db, nil
}

// Handlers for HTTP requests
// Modify handlers to accept *sql.DB

func handleGetUsers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		query := r.URL.Query()
		idParam := query.Get("id")

		if idParam != "" {
			targetID, err := strconv.Atoi(idParam)
			if err != nil {
				http.Error(w, "Invalid user ID format", http.StatusBadRequest)
				return
			}

			// Use the passed-in db instance
			dbRows, err := db.Query("SELECT id, name, email, age FROM users WHERE id = ?", targetID)
			if err != nil {
				log.Printf("Error querying user by ID %d: %v", targetID, err)
				http.Error(w, "Internal server error (DB query)", http.StatusInternalServerError)
				return
			}
			defer dbRows.Close()

			foundUser := false
			for dbRows.Next() {
				foundUser = true
				var user User
				err := dbRows.Scan(&user.ID, &user.Name, &user.Email, &user.Age)
				if err != nil {
					log.Printf("Error scanning user row for ID %d: %v", targetID, err)
					http.Error(w, "Internal server error (DB scan)", http.StatusInternalServerError)
					return
				}
				// For now, just printing the first found user. Consider JSON response.
				fmt.Fprintf(w, "ID: %d, Name: %s, Email: %s, Age: %d\n", user.ID, user.Name, user.Email, user.Age)
			}
			if err := dbRows.Err(); err != nil {
				log.Printf("Error iterating rows for ID %d: %v", targetID, err)
				http.Error(w, "Internal server error (DB iteration)", http.StatusInternalServerError)
				return
			}

			if !foundUser {
				http.Error(w, fmt.Sprintf("User with ID %d not found", targetID), http.StatusNotFound)
			}
			return
		}

		// If no ID parameter, return all users
		rows, err := db.Query("SELECT id, name, email, age FROM users")
		if err != nil {
			log.Printf("Error querying all users: %v", err)
			http.Error(w, "Error querying users from DB", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Consider returning JSON array
		fmt.Fprintf(w, "Users:\n")
		for rows.Next() {
			var u User
			err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.Age)
			if err != nil {
				log.Printf("Error scanning user row (all users): %v", err)
				// Decide if you want to stop or continue
				http.Error(w, "Error scanning user data", http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "ID: %d, Name: %s, Email: %s, Age: %d\n", u.ID, u.Name, u.Email, u.Age)
		}
		if err := rows.Err(); err != nil {
			log.Printf("Error iterating all users rows: %v", err)
			http.Error(w, "Error iterating user data", http.StatusInternalServerError)
		}
	}
}

func handleAddUser(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		query := r.URL.Query()
		name := query.Get("name")
		email := query.Get("email")
		ageStr := query.Get("age") // Assuming age might be passed

		if name == "" || email == "" {
			http.Error(w, "Name and Email are required", http.StatusBadRequest)
			return
		}
		// Basic validation for query parameters count (adjust as needed)
		// if len(query) < 2 || len(query) > 3 {
		// 	http.Error(w, "Invalid parameters: name, email, and optional age are allowed.", http.StatusBadRequest)
		// 	return
		// }

		var age int
		var err error
		if ageStr != "" {
			age, err = strconv.Atoi(ageStr)
			if err != nil {
				http.Error(w, "Invalid age format", http.StatusBadRequest)
				return
			}
		}

		// Use the passed-in db instance
		stmt, err := db.Prepare("INSERT INTO users(name, email, age) values(?,?,?)")
		if err != nil {
			log.Printf("Error preparing insert statement: %v", err)
			http.Error(w, "Internal server error (DB prepare)", http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		result, err := stmt.Exec(name, email, age)
		if err != nil {
			log.Printf("Error executing insert statement: %v", err)
			http.Error(w, "Internal server error (DB exec)", http.StatusInternalServerError)
			return
		}

		lastID, err := result.LastInsertId()
		if err != nil {
			log.Printf("Error getting last insert ID: %v", err)
			// User was inserted, but we can't get the ID.
			http.Error(w, "Internal server error (ID retrieval)", http.StatusInternalServerError)
			return
		}
		// Consider returning JSON
		fmt.Fprintf(w, "User added: ID %d, Name %s, Email %s, Age %d\n", int(lastID), name, email, age)
	}
}

func main() {
	// Initialize database
	db, err := newDB(dbFileName)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err) // Log and exit if DB setup fails
	}
	defer db.Close() // Ensure database is closed when main exits

	// Pass the db instance to the handlers
	http.HandleFunc("/users", handleGetUsers(db))
	http.HandleFunc("/add", handleAddUser(db))

	fmt.Println("Server starting on :8080, using SQLite backend.")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		// http.ListenAndServe always returns a non-nil error.
		// If it's http.ErrServerClosed, it's a graceful shutdown.
		if err == http.ErrServerClosed {
			log.Println("Server closed gracefully.")
		} else {
			log.Printf("Server failed: %v", err)
		}
	}
}
