# Project Overview

This project demonstrates a data processing pipeline with a Python-based API and Go-based data manipulation and ingestion tools.

## Getting Started

### Prerequisites

- Go (version 1.24 or later recommended)
- Python (version 3.12 or later recommended)
- Poetry (for Python dependency management)

### Setup & Running

1.  **Clone the repository:**

    ```sh
    git clone https://github.com/kagenihisomi/gogogo
    cd gogogo
    ```

2.  **Setup CRUD API and ingestion**
    Navigate to the `userator` directory, install dependencies using Poetry, and run the Uvicorn server for the FastAPI application.

    ```sh
    poetry install
    poetry run uvicorn userator.api_1:app --reload
    ```

    The API will typically be available at `http://localhost:8000`. The [`users.db`](users.db) SQLite database will be created automatically if it doesn't exist.

    This command fetches data from the Python API and saves it locally. Open a new terminal.

    ```sh
    go run ./cmd/ingest/main.go
    ```

    This will create `tmp/users.json` and `tmp/users_simple.parquet`.

3.  **Run the Go `writer` command:**
    This command demonstrates the `datarizer` package by parsing a sample dataset and writing it to files.
    ```sh
    go run ./cmd/writer/main.go
    ```
    This will create `tmp/students.jsonl` and `tmp/students.parquet`.

## Core Components

### 1. User API (Python FastAPI)

The primary API for managing user data is implemented in Python using the FastAPI framework and served with Uvicorn.

- **Source Code**: [`userator/api_1.py`](userator/api_1.py)
- **Functionality**:
  - CRUD operations for users (Create, Read).
  - SQLite database backend ([`users.db`](users.db)).
  - Data validation using Pydantic models.
  - Pagination for listing users.
- **Testing**: Unit and integration tests are provided in [`userator/test_api1.py`](userator/test_api1.py) using `pytest` and `TestClient`.
- **Dependencies**: Managed by Poetry ([`pyproject.toml`](pyproject.toml), [`poetry.lock`](poetry.lock)).

### 2. Go `datarizer` Package

A Go package for data transformation and handling, primarily focused on DataFrame-like operations.

- **Source Code**: [`pkg/datarizer/dataframe.go`](pkg/datarizer/dataframe.go)
- **Functionality**:
  - **DataFrame Abstraction**: Generic `DataFrame[T]` structure to hold records.
  - **Parquet Support**:
    - Write DataFrames to local Parquet files ([`WriteToLocalParquet`](pkg/datarizer/dataframe.go)).
    - Read DataFrames from local Parquet files ([`ReadFromLocalParquet`](pkg/datarizer/dataframe.go)).
    - Write DataFrames to S3-compatible storage as Parquet files ([`WriteToS3Parquet`](pkg/datarizer/dataframe.go)).
    - Read DataFrames from S3-compatible Parquet files ([`ReadFromS3Parquet`](pkg/datarizer/dataframe.go)).
  - **JSONL Support**:
    - Write DataFrames to local JSONL files ([`WriteToJSONL`](pkg/datarizer/dataframe.go)).
    - Read DataFrames from local JSONL files ([`ReadFromJSONL`](pkg/datarizer/dataframe.go)).
  - **Schema Parsing**: Includes a `BaseSchemaParser` ([`BaseSchemaParser`](pkg/datarizer/dataframe.go)) to parse JSON data and enrich it with `RecordInfo` (metadata like raw data, hash, timestamp, source).
- **Testing**: Comprehensive tests for local and S3 Parquet/JSONL operations, including MinIO for S3 testing, are in [`pkg/datarizer/dataframe_test.go`](pkg/datarizer/dataframe_test.go).
- **Dependencies**: Managed via Go modules ([`pkg/go.mod`](pkg/go.mod)).

### 3. Go `ingest` Command

A command-line tool to fetch user data from the Python API and save it to local files.

- **Source Code**: [`cmd/ingest/main.go`](cmd/ingest/main.go)
- **Functionality**:
  - Fetches all users from the `/users/` endpoint of the FastAPI application, handling pagination.
  - Implements retry logic with backoff for HTTP requests using `go-retryablehttp`.
  - Saves the fetched data as a JSON file (`tmp/users.json`) and a Parquet file (`tmp/users_simple.parquet`).

### 4. Go `writer` Command

A command-line tool that demonstrates the usage of the `datarizer` package.

- **Source Code**: [`cmd/writer/main.go`](cmd/writer/main.go)
- **Functionality**:
  - Parses a predefined JSON dataset into `Student` structs (defined in [`pkg/datarizer/dataframe.go`](pkg/datarizer/dataframe.go)) using `datarizer.BaseSchemaParser`.
  - Writes the parsed data to a JSONL file (`tmp/students.jsonl`) and a Parquet file (`tmp/students.parquet`) using the `datarizer` DataFrame methods.

### 5. Deprecated Go API (v1)

An older version of the User API implemented in Go. This is considered deprecated in favor of the Python FastAPI version.

- **Source Code**: [`cmd/api/v1/main.go`](cmd/api/v1/main.go)
- **Functionality**: Basic CRUD operations for users with an SQLite backend.
- **Testing**: Unit tests are available in [`cmd/api/v1/main_test.go`](cmd/api/v1/main_test.go).

## Development & CI/CD

- **Live Reload (Go API)**: The `.air.toml` file ([`.air.toml`](.air.toml)) is configured for live reloading of the Go API during development.
- **Pre-commit Hooks**: Configured in [`.pre-commit-config.yaml`](.pre-commit-config.yaml) for Go, including `go-build`, `go-mod-tidy`, and `golangci-lint`.
- **GitHub Actions Workflows**:
  - **Go CI**: Linting, testing, and building for the Go components ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).
  - **Python CI**: Linting (flake8, black, isort) and testing (pytest) for the Python components ([`.github/workflows/ci_py.yml`](.github/workflows/ci_py.yml)).
- **Go Linting Configuration**: Defined in [`.golangci.yml`](.golangci.yml).

## Other Interesting Points

- **Workspace Structure**: The project is organized into `cmd/` for Go executables, `pkg/` for shared Go libraries (like `datarizer`), and `userator/` for the Python API.
- **Database**: SQLite is used for both the Python API ([`users.db`](users.db)) and the deprecated Go API.
- **Temporary Files**: The `tmp/` directory is used for output files generated by the `ingest` and `writer` commands, as well as by some tests. It is included in the [`.gitignore`](.gitignore) file.
- **Go Modules**: The root Go project ([`go.mod`](go.mod)) uses a `replace` directive to point to the local `pkg/` directory for the `datarizer` module.
