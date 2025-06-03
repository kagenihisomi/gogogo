package main

import (
	"bufio"
	"encoding/json" // New: For JSON output
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync" // For mutex
)

// User struct
type User struct {
	ID    int    `json:"id"` // Add JSON tags
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserService manages user data
type UserService struct {
	mu     sync.Mutex // Mutex to protect users slice
	users  []User
	nextID int
	dbPath string // Path to the file (more general than "fileName")
}

// NewUserService creates and initializes a new UserService
func NewUserService(dbPath string) (*UserService, error) {
	s := &UserService{
		dbPath: dbPath,
		nextID: 1, // Start ID from 1
	}
	if err := s.loadUsers(); err != nil {
		return nil, fmt.Errorf("failed to load users during service initialization: %w", err)
	}
	return s, nil
}

// loadUsers loads users from a file (now a method of UserService)
func (s *UserService) loadUsers() error {
	s.mu.Lock() // Lock before accessing shared state
	defer s.mu.Unlock()

	file, err := os.Open(s.dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File '%s' does not exist, starting with empty user list.", s.dbPath)
			s.users = []User{} // Ensure users slice is initialized
			return nil
		}
		return fmt.Errorf("error opening file '%s': %w", s.dbPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", s.dbPath, closeErr)
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
			s.users = append(s.users, User{ID: id, Name: parts[1], Email: parts[2]})
			if id >= s.nextID {
				s.nextID = id + 1 // Ensure nextID is always greater than existing IDs
			}
		} else {
			log.Printf("Skipping invalid user line (incorrect parts count): %s", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from file '%s': %w", s.dbPath, err)
	}
	return nil
}

// saveUsers saves users to a file (now a method of UserService)
func (s *UserService) saveUsers() error {
	s.mu.Lock() // Lock before accessing shared state
	defer s.mu.Unlock()

	file, err := os.Create(s.dbPath)
	if err != nil {
		return fmt.Errorf("error creating file '%s': %w", s.dbPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", s.dbPath, closeErr)
		}
	}()

	for _, user := range s.users {
		_, err := fmt.Fprintf(file, "%d,%s,%s\n", user.ID, user.Name, user.Email)
		if err != nil {
			return fmt.Errorf("error writing user %d to file '%s': %w", user.ID, s.dbPath, err)
		}
	}
	return nil
}

// GetUsers returns all users (reads protected by mutex)
func (s *UserService) GetUsers() []User {
	s.mu.Lock() // Lock for read, but copy to avoid modification outside
	defer s.mu.Unlock()
	// Return a copy to prevent external modification of the internal slice
	usersCopy := make([]User, len(s.users))
	copy(usersCopy, s.users)
	return usersCopy
}

// AddUser adds a new user and saves to file
func (s *UserService) AddUser(name, email string) (User, error) {
	s.mu.Lock() // Lock for write
	defer s.mu.Unlock()

	newUser := User{ID: s.nextID, Name: name, Email: email}
	s.nextID++
	s.users = append(s.users, newUser)

	if err := s.saveUsers(); err != nil { // Still saving on every add
		return User{}, fmt.Errorf("failed to save users after adding new user: %w", err)
	}
	return newUser, nil
}

// HTTP Handler methods (now receive UserService)
func (s *UserService) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	users := s.GetUsers()                                    // Get users from service
	w.Header().Set("Content-Type", "application/json")       // Set content type
	if err := json.NewEncoder(w).Encode(users); err != nil { // Encode to JSON
		log.Printf("Error encoding users to JSON: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *UserService) handleAddUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
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

	newUser, err := s.AddUser(name, email)
	if err != nil {
		log.Printf("Failed to add user: %v", err)
		http.Error(w, "Failed to add user", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(newUser); err != nil {
		log.Printf("Error encoding new user to JSON: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func main() {
	// Initialize the service
	service, err := NewUserService(fileName)
	if err != nil {
		log.Fatalf("Failed to initialize user service: %v", err)
	}

	// Register handlers with the service instance
	http.HandleFunc("/users", service.handleGetUsers)
	http.HandleFunc("/add", service.handleAddUser)

	log.Println("Server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
