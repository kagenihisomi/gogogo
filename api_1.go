package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// User struct
type User struct {
	ID    int
	Name  string
	Email string
}

// Global variable to store users (bad practice)
var users []User

// fileName where data is stored
const fileName = "users.txt"

// init function to load data on startup (can be problematic)
func init() {
	loadUsersFromFile()
}

// loadUsersFromFile loads users from a file
func loadUsersFromFile() {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error opening file:", err) // Just print, no proper handling
		return
	}
	defer file.Close() // Defer but no error check

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) == 3 {
			id, _ := strconv.Atoi(parts[0]) // Ignore error
			users = append(users, User{ID: id, Name: parts[1], Email: parts[2]})
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err) // Just print, no proper handling
	}
}

// saveUsersToFile saves users to a file
func saveUsersToFile() {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating file:", err) // Just print
		return
	}
	defer file.Close() // Defer but no error check

	for _, user := range users {
		_, err := fmt.Fprintf(file, "%d,%s,%s\n", user.ID, user.Name, user.Email)
		if err != nil {
			fmt.Println("Error writing to file:", err) // Just print
		}
	}
}

// Handlers for HTTP requests
func handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
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

	if name == "" || email == "" {
		http.Error(w, "Name and Email are required", http.StatusBadRequest)
		return
	}

	// Simple ID generation (problematic in concurrent scenarios)
	newID := len(users) + 1
	newUser := User{ID: newID, Name: name, Email: email}
	users = append(users, newUser)
	saveUsersToFile() // Saves every time, not efficient

	fmt.Fprintf(w, "User added: ID %d, Name %s, Email %s\n", newUser.ID, newUser.Name, newUser.Email)
}

func main() {
	http.HandleFunc("/users", handleGetUsers)
	http.HandleFunc("/add", handleAddUser)

	fmt.Println("Server starting on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Server failed:", err) // No proper error handling
	}
}
