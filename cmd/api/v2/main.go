package main

import (
	"bufio"
	"fmt"
	"log" // Use log for structured error logging
	"net/http"
	"os"
	"strconv"
	"strings"
	// For mutex in later steps
)

// User struct
type User struct {
	ID    int
	Name  string
	Email string
}

// Global variable to store users (still bad, but will be fixed later)
var users []User
var nextID int = 1 // Track next ID

// fileName where data is stored
const fileName = "users.txt"

// loadUsersFromFile loads users from a file
func loadUsersFromFile() error {
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) { // Handle file not existing gracefully
			log.Printf("File '%s' does not exist, starting with empty user list.", fileName)
			return nil
		}
		return fmt.Errorf("error opening file '%s': %w", fileName, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", fileName, closeErr) // Log close error, don't return
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) == 3 {
			id, err := strconv.Atoi(parts[0])
			if err != nil {
				log.Printf("Skipping invalid user line (ID not integer): %s - %v", line, err)
				continue
			}
			users = append(users, User{ID: id, Name: parts[1], Email: parts[2]})
			if id >= nextID {
				nextID = id + 1 // Ensure nextID is always greater than existing IDs
			}
		} else {
			log.Printf("Skipping invalid user line (incorrect parts count): %s", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from file '%s': %w", fileName, err)
	}
	return nil
}

// saveUsersToFile saves users to a file
func saveUsersToFile() error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error creating file '%s': %w", fileName, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", fileName, closeErr)
		}
	}()

	for _, user := range users {
		_, err := fmt.Fprintf(file, "%d,%s,%s\n", user.ID, user.Name, user.Email)
		if err != nil {
			return fmt.Errorf("error writing user %d to file '%s': %w", user.ID, fileName, err)
		}
	}
	return nil
}

// Handlers for HTTP requests (still direct, will improve later)
func handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { // Use http.Method constants
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	// Still plain text output
	fmt.Fprintf(w, "Users:\n")
	for _, user := range users {
		fmt.Fprintf(w, "ID: %d, Name: %s, Email: %s\n", user.ID, user.Name, user.Email)
	}
}

func handleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // Use http.Method constants
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	name := query.Get("name")
	email := query.Get("email")

	if name == "" || email == "" {
		http.Error(w, "Name and Email are required", http.StatusBadRequest)
		return
	}

	newUser := User{ID: nextID, Name: name, Email: email}
	nextID++ // Increment ID
	users = append(users, newUser)

	if err := saveUsersToFile(); err != nil { // Check save error
		log.Printf("Failed to save users after adding new user: %v", err)
		http.Error(w, "Failed to save user data", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "User added: ID %d, Name %s, Email %s\n", newUser.ID, newUser.Name, newUser.Email)
}

func main() {
	if err := loadUsersFromFile(); err != nil {
		log.Fatalf("Fatal error loading users: %v", err) // Use log.Fatalf for unrecoverable errors
	}

	http.HandleFunc("/users", handleGetUsers)
	http.HandleFunc("/add", handleAddUser)

	log.Println("Server starting on :8080") // Use log for server start message
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err) // Use log.Fatalf
	}
}
