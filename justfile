set dotenv-load

db := "postgres://chatbridge:chatbridge@localhost:5432/chatbridge"
redis := "redis://localhost:6379"

# List available recipes
default:
    @just --list

# Build the project
build:
    cargo build

# Run the server (requires env vars)
run:
    cargo run

# Format code
fmt:
    cargo fmt

# Check formatting without modifying
fmt-check:
    cargo fmt --check

# Run clippy lints
lint:
    cargo clippy

# Run unit tests (no database needed)
test-unit:
    cargo test --lib

# Run all tests (unit + integration, requires Postgres and Redis)
test: db-up redis-up
    DATABASE_URL={{ db }} REDIS_URL={{ redis }} cargo test

# Run a single test by name
test-one name: db-up redis-up
    DATABASE_URL={{ db }} REDIS_URL={{ redis }} cargo test {{ name }}

# Start Postgres container
db-up:
    docker compose up -d postgres

# Stop Postgres container
db-down:
    docker compose down postgres

# Start Redis container
redis-up:
    docker compose up -d redis

# Stop Redis container
redis-down:
    docker compose down redis

# Start full stack (chatbridge + postgres)
up:
    docker compose up --build

# Start full stack in background
upd:
    docker compose up --build -d

# Stop all containers
down:
    docker compose down

# Run fmt, lint, and tests
check: fmt lint test
