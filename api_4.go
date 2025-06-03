package main

import (
	"bufio"
	"context" // New: For graceful shutdown
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal" // New: For graceful shutdown
	"strconv"
	"strings"
	"sync"
	"syscall" // New: For graceful shutdown
	"time"    // New: For graceful shutdown timeout
)

// User struct
type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserStore interface defines operations for user persistence
type UserStore interface {
	LoadUsers() ([]User, error)
	SaveUsers([]User) error
}

// FileStore implements UserStore for file-based persistence
type FileStore struct {
	dbPath string
}

// NewFileStore creates a new FileStore instance
func NewFileStore(dbPath string) *FileStore {
	return &FileStore{dbPath: dbPath}
}

// LoadUsers implements UserStore interface for FileStore
func (fs *FileStore) LoadUsers() ([]User, error) {
	file, err := os.Open(fs.dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File '%s' does not exist, starting with empty user list.", fs.dbPath)
			return []User{}, nil // Return empty slice if file doesn't exist
		}
		return nil, fmt.Errorf("error opening file '%s': %w", fs.dbPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", fs.dbPath, closeErr)
		}
	}()

	var users []User
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
		} else {
			log.Printf("Skipping invalid user line (incorrect parts count): %s", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file '%s': %w", fs.dbPath, err)
	}
	return users, nil
}

// SaveUsers implements UserStore interface for FileStore
func (fs *FileStore) SaveUsers(users []User) error {
	file, err := os.Create(fs.dbPath)
	if err != nil {
		return fmt.Errorf("error creating file '%s': %w", fs.dbPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("Error closing file '%s': %v", fs.dbPath, closeErr)
		}
	}()

	for _, user := range users {
		_, err := fmt.Fprintf(file, "%d,%s,%s\n", user.ID, user.Name, user.Email)
		if err != nil {
			return fmt.Errorf("error writing user %d to file '%s': %w", user.ID, fs.dbPath, err)
		}
	}
	return nil
}

// UserService manages user data using a UserStore
type UserService struct {
	mu     sync.Mutex
	users  []User
	nextID int
	store  UserStore // Dependency on the UserStore interface
}

// NewUserService creates and initializes a new UserService
func NewUserService(store UserStore) (*UserService, error) {
	s := &UserService{
		store:  store,
		nextID: 1,
	}
	initialUsers, err := store.LoadUsers() // Load users via the store interface
	if err != nil {
		return nil, fmt.Errorf("failed to load users during service initialization: %w", err)
	}
	s.users = initialUsers
	// Find the max ID to set nextID correctly
	for _, user := range initialUsers {
		if user.ID >= s.nextID {
			s.nextID = user.ID + 1
		}
	}
	return s, nil
}

// GetUsers returns all users
func (s *UserService) GetUsers() []User {
	s.mu.Lock()
	defer s.mu.Unlock()
	usersCopy := make([]User, len(s.users))
	copy(usersCopy, s.users)
	return usersCopy
}

// AddUser adds a new user and saves to store
func (s *UserService) AddUser(name, email string) (User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newUser := User{ID: s.nextID, Name: name, Email: email}
	s.nextID++
	s.users = append(s.users, newUser)

	// Save all users via the store
	if err := s.store.SaveUsers(s.users); err != nil {
		return User{}, fmt.Errorf("failed to save users after adding new user: %w", err)
	}
	return newUser, nil
}

// HTTP Handler methods
func (s *UserService) handleGetUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	users := s.GetUsers()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(users); err != nil {
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
	// Setup storage
	fileStore := NewFileStore(fileName)

	// Initialize the service with the store
	service, err := NewUserService(fileStore)
	if err != nil {
		log.Fatalf("Failed to initialize user service: %v", err)
	}

	// Setup HTTP server
	mux := http.NewServeMux() // Use NewServeMux for better routing
	mux.HandleFunc("/users", service.handleGetUsers)
	mux.HandleFunc("/add", service.handleAddUser)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,             // Pass the mux here
		ReadTimeout:  5 * time.Second, // Add timeouts
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Create a channel to listen for OS signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // Listen for Ctrl+C and termination signals

	// Run server in a goroutine
	go func() {
		log.Println("Server starting on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Block until we receive a signal
	<-quit
	log.Println("Shutting down server...")

	// Create a context with a timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // Ensure the context is cancelled

	// Attempt to gracefully shut down the server
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server exited gracefully.")
}
