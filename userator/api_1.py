import sqlite3
from typing import List, Optional
from contextlib import asynccontextmanager  # Import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, status, Query
from pydantic import BaseModel, EmailStr, Field

DATABASE_URL = "users.db"


# --- Pydantic Models (Similar to Go's User struct + request/response shaping) ---
class UserBase(BaseModel):
    name: str
    email: EmailStr
    age: Optional[int] = Field(default=0, ge=0)  # ge=0 for non-negative age


class UserCreate(UserBase):
    pass  # For creating a user


class UserResponse(UserBase):
    id: int

    class Config:
        from_attributes = True  # Allows Pydantic to create UserResponse from ORM-like objects (e.g. dicts from db rows)


# --- Database Setup and Dependency Injection ---
def create_db_and_tables():
    """Initializes the database and creates the users table if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            age INTEGER DEFAULT 0
        )
    """
    )
    conn.commit()
    conn.close()


# Dependency: This function will be called by FastAPI for each request
# that declares a dependency on it.
def get_db_connection():
    """
    Opens a new database connection for the duration of a request.
    FastAPI will ensure this is called per request needing it,
    and the 'finally' block ensures the connection is closed.
    """
    db = sqlite3.connect(DATABASE_URL)
    db.row_factory = sqlite3.Row  # Access columns by name
    # Optimize for write performance
    db.execute("PRAGMA journal_mode = WAL;")
    db.execute("PRAGMA synchronous = NORMAL;")
    try:
        yield db  # This is what gets injected into your path operation functions
    finally:
        db.close()  # Ensures connection is closed after request processing


# --- Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on application startup
    create_db_and_tables()
    print("Database and tables initialized.")
    yield
    # Code to run on application shutdown (if any)
    # print("Application shutting down.")


app = FastAPI(
    title="User API (FastAPI Refactor)", lifespan=lifespan
)  # Pass the lifespan manager


# --- Path Operations (Handlers) ---


# Equivalent to Go's handleAddUser
@app.post(
    "/users/",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Users"],
)
def add_user(
    user_in: UserCreate,  # Request body will be parsed into UserCreate model
    db: sqlite3.Connection = Depends(get_db_connection),  # Dependency Injection
):
    """
    Add a new user.
    - **name**: User's name (required)
    - **email**: User's email (required, must be unique)
    - **age**: User's age (optional, defaults to 0)
    """
    try:
        cursor = db.execute(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            (user_in.name, user_in.email, user_in.age),
        )
        db.commit()
        created_user_id = cursor.lastrowid
        # Return the created user data conforming to UserResponse
        return UserResponse(
            id=created_user_id, name=user_in.name, email=user_in.email, age=user_in.age
        )
    except sqlite3.IntegrityError as e:  # Catch UNIQUE constraint violation
        db.rollback()
        if "UNIQUE constraint failed: users.email" in str(e):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Email '{user_in.email}' already exists.",
            )
        else:
            # Log other IntegrityErrors if necessary
            print(f"Database IntegrityError on add_user: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="A database integrity error occurred.",
            )
    except sqlite3.Error as e:  # Catch other SQLite errors
        db.rollback()
        # Log the error e
        print(f"Database error on add_user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal error occurred while adding the user.",
        )


# Equivalent to Go's handleGetUsers (combined logic for all users and specific user)
@app.get("/users/", response_model=List[UserResponse], tags=["Users"])
def get_users(
    user_id: Optional[int] = Query(
        None, description="Optional ID of the user to retrieve"
    ),
    skip: int = Query(
        0,
        ge=0,
        description="Offset: Number of items to skip for pagination when listing all users.",
    ),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="Limit: Maximum number of items to return per page when listing all users.",
    ),
    db: sqlite3.Connection = Depends(get_db_connection),
):
    """
    Retrieve users.
    - If **user_id** is provided, retrieves a specific user (skip and limit are ignored).
    - Otherwise, retrieves a list of all users using skip/limit pagination.
    """
    if user_id is not None:
        # Logic for fetching a single user by ID
        cursor = db.execute(
            "SELECT id, name, email, age FROM users WHERE id = ?", (user_id,)
        )
        user_row = cursor.fetchone()
        if user_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with ID {user_id} not found",
            )
        # Return as a list with one item for consistency with response_model=List[UserResponse]
        # Or, you could have a separate endpoint for single user that returns UserResponse directly
        return [UserResponse.model_validate(dict(user_row))]
    else:
        # Logic for fetching all users with LIMIT/OFFSET pagination
        # ORDER BY is crucial for consistent pagination
        query = "SELECT id, name, email, age FROM users ORDER BY id LIMIT ? OFFSET ?"
        cursor = db.execute(query, (limit, skip))
        users_rows = cursor.fetchall()
        return [UserResponse.model_validate(dict(row)) for row in users_rows]


# If you want a separate endpoint for getting a user by ID (more RESTful):
@app.get("/users/{user_id_path}", response_model=UserResponse, tags=["Users"])
def get_user_by_id(
    user_id_path: int,  # Path parameter
    db: sqlite3.Connection = Depends(get_db_connection),
):
    """
    Retrieve a specific user by their ID.
    """
    cursor = db.execute(
        "SELECT id, name, email, age FROM users WHERE id = ?", (user_id_path,)
    )
    user_row = cursor.fetchone()
    if user_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID {user_id_path} not found",
        )
    return UserResponse.model_validate(dict(user_row))


# To run this: uvicorn api_fastapi_refactor:app --reload
