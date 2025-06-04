import pytest
import sqlite3
from typing import List, Dict, Any
from fastapi.testclient import TestClient  # Import TestClient

# Assuming your FastAPI app and Pydantic models are in api_1.py
# You might need to adjust the import path if your project structure is different
# For example, if gogogo is a package: from gogogo.api_1 import app, UserCreate, UserResponse, get_db_connection
from api_1 import app, UserCreate, UserResponse, get_db_connection, DATABASE_URL

# --- Test Database Setup ---
# We'll use an in-memory SQLite database for tests.
# The tables will be created for each test function that uses the 'test_db' fixture.


@pytest.fixture(scope="function")
def test_db_conn():
    """
    Fixture to set up an in-memory SQLite database for a single test function.
    Creates tables and yields a connection. Closes connection afterwards.
    Disables thread checking for test environment compatibility with FastAPI's TestClient.
    """
    # Using ":memory:" creates a fresh DB for each connection in sqlite3 by default.
    # Add check_same_thread=False to allow the connection to be used across
    # the fixture thread and the FastAPI endpoint's worker thread during testing.
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Important for accessing columns by name

    # Create tables (mirroring create_db_and_tables from api_1.py)
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
    yield conn  # Provide the connection to the test
    conn.close()


@pytest.fixture(scope="function")
def client(test_db_conn: sqlite3.Connection):
    """
    Fixture to provide a TestClient with the get_db_connection dependency overridden.
    """

    def override_get_db_connection():
        try:
            yield test_db_conn  # Use the connection from the test_db_conn fixture
        finally:
            # The test_db_conn fixture is responsible for closing the connection
            pass

    app.dependency_overrides[get_db_connection] = override_get_db_connection
    # Use TestClient for FastAPI testing
    with TestClient(app) as c:  # Changed from TestClient
        yield c
    # Clean up dependency overrides
    del app.dependency_overrides[get_db_connection]


# --- Test Cases ---


# Tests for Add User (POST /users/)
def test_add_user_positive(client: TestClient, test_db_conn: sqlite3.Connection):
    """Positive case - add user successfully"""
    user_data = {"name": "Test User", "email": "test@example.com", "age": 30}
    response = client.post("/users/", json=user_data)

    assert response.status_code == 201, response.text
    response_data = response.json()
    assert response_data["name"] == user_data["name"]
    assert response_data["email"] == user_data["email"]
    assert response_data["age"] == user_data["age"]
    assert "id" in response_data

    # Verify in DB
    cursor = test_db_conn.cursor()
    cursor.execute(
        "SELECT name, email, age FROM users WHERE email = ?", (user_data["email"],)
    )
    db_user = cursor.fetchone()
    assert db_user is not None
    assert db_user["name"] == user_data["name"]
    assert db_user["age"] == user_data["age"]


def test_add_user_duplicate_email(client: TestClient):
    """Negative case - email already exists"""
    user_data = {"name": "First User", "email": "duplicate@example.com", "age": 25}
    client.post("/users/", json=user_data)  # Add first user

    user_data_dup = {"name": "Second User", "email": "duplicate@example.com", "age": 35}
    response = client.post("/users/", json=user_data_dup)

    assert response.status_code == 409  # Conflict
    assert "already exists" in response.json()["detail"]


def test_add_user_missing_name(client: TestClient):
    """Negative case - missing name (Pydantic validation)"""
    user_data = {"email": "noname@example.com", "age": 25}
    response = client.post("/users/", json=user_data)
    assert response.status_code == 422  # Unprocessable Entity for Pydantic validation
    response_data = response.json()
    assert any(
        err["type"] == "missing" and "name" in err["loc"]
        for err in response_data["detail"]
    )


def test_add_user_missing_email(client: TestClient):
    """Negative case - missing email (Pydantic validation)"""
    user_data = {"name": "No Email User", "age": 25}
    response = client.post("/users/", json=user_data)
    assert response.status_code == 422
    response_data = response.json()
    assert any(
        err["type"] == "missing" and "email" in err["loc"]
        for err in response_data["detail"]
    )


def test_add_user_invalid_email_format(client: TestClient):
    """Negative case - invalid email format (Pydantic validation)"""
    user_data = {"name": "Bad Email", "email": "not-an-email", "age": 30}
    response = client.post("/users/", json=user_data)
    assert response.status_code == 422
    response_data = response.json()
    assert any("email" in err["loc"] for err in response_data["detail"])


def test_add_user_invalid_age_type(client: TestClient):
    """Negative case - invalid age type (Pydantic validation)"""
    user_data = {"name": "Bad Age Type", "email": "badage@example.com", "age": "thirty"}
    response = client.post("/users/", json=user_data)
    assert response.status_code == 422
    response_data = response.json()
    assert any(
        "int_parsing" in err["type"] and "age" in err["loc"]
        for err in response_data["detail"]
    )


def test_add_user_negative_age(client: TestClient):
    """Negative case - age less than 0 (Pydantic validation ge=0)"""
    user_data = {"name": "Negative Age", "email": "negage@example.com", "age": -5}
    response = client.post("/users/", json=user_data)
    assert response.status_code == 422
    response_data = response.json()
    assert any(
        "greater_than_equal" in err["type"] and "age" in err["loc"]
        for err in response_data["detail"]
    )


# Tests for Get Users (GET /users/ and GET /users/{user_id_path})
def _add_sample_users(db_conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    users_data = [
        {"name": "Alice", "email": "alice@example.com", "age": 28},
        {"name": "Bob", "email": "bob@example.com", "age": 32},
    ]
    inserted_users = []
    cursor = db_conn.cursor()
    for user in users_data:
        cursor.execute(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            (user["name"], user["email"], user["age"]),
        )
        user_id = cursor.lastrowid
        inserted_users.append({**user, "id": user_id})
    db_conn.commit()
    return inserted_users


def test_get_all_users(client: TestClient, test_db_conn: sqlite3.Connection):
    """Positive case - get all users"""
    sample_users = _add_sample_users(test_db_conn)
    response = client.get("/users/")

    assert response.status_code == 200
    response_data = response.json()
    assert len(response_data) == len(sample_users)

    # Check if all sample users are in the response (order might not be guaranteed)
    response_emails = {u["email"] for u in response_data}
    sample_emails = {u["email"] for u in sample_users}
    assert response_emails == sample_emails


def test_get_specific_user_by_id_path(
    client: TestClient, test_db_conn: sqlite3.Connection
):
    """Positive case - get specific user by path parameter ID"""
    sample_users = _add_sample_users(test_db_conn)
    user_to_get = sample_users[0]  # Get Alice

    response = client.get(f"/users/{user_to_get['id']}")
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["id"] == user_to_get["id"]
    assert response_data["name"] == user_to_get["name"]
    assert response_data["email"] == user_to_get["email"]


def test_get_specific_user_by_id_query(
    client: TestClient, test_db_conn: sqlite3.Connection
):
    """Positive case - get specific user by query parameter ID"""
    sample_users = _add_sample_users(test_db_conn)
    user_to_get = sample_users[1]  # Get Bob

    response = client.get(f"/users/?user_id={user_to_get['id']}")
    assert response.status_code == 200
    response_data = response.json()
    assert isinstance(response_data, list)
    assert len(response_data) == 1
    user_in_list = response_data[0]
    assert user_in_list["id"] == user_to_get["id"]
    assert user_in_list["name"] == user_to_get["name"]
    assert user_in_list["email"] == user_to_get["email"]


def test_get_user_not_found_path(client: TestClient):
    """Negative case - get specific user by path, ID not found"""
    response = client.get("/users/99999")  # Non-existent ID
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_get_user_not_found_query(client: TestClient):
    """Negative case - get specific user by query, ID not found"""
    response = client.get("/users/?user_id=99999")  # Non-existent ID
    assert response.status_code == 404  # As per current Python code, this raises 404
    assert "not found" in response.json()["detail"]


def test_get_user_invalid_id_path(client: TestClient):
    """Negative case - get specific user by path, invalid ID format"""
    response = client.get("/users/abc")
    assert response.status_code == 422  # FastAPI validation for path param type
    response_data = response.json()
    assert any(
        "int_parsing" in err["type"] and "user_id_path" in err["loc"]
        for err in response_data["detail"]
    )


def test_get_user_invalid_id_query(client: TestClient):
    """Negative case - get specific user by query, invalid ID format"""
    response = client.get("/users/?user_id=abc")
    assert response.status_code == 422  # FastAPI validation for query param type
    response_data = response.json()
    assert any(
        "int_parsing" in err["type"] and "user_id" in err["loc"]
        for err in response_data["detail"]
    )
