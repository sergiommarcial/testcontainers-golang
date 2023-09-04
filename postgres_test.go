package test

import (
	"context"
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgresContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()

	// Define a PostgreSQL container
	container, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("docker.io/postgres:latest"),
		postgres.WithDatabase("example-db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Create the container
	connStr, err := container.ConnectionString(ctx, "sslmode=disable", "application_name=test")
	assert.NoError(t, err)

	return container, connStr
}

func TestPostgreSQLIntegration(t *testing.T) {
	// Start the PostgreSQL container
	container, connectionString := setupPostgresContainer(t)
	defer container.Terminate(context.Background())

	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		t.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Perform your test operations
	// For example, create a table and insert some data
	_, err = db.Exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255));")
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name) VALUES ($1), ($2);", "Alice", "Bob")
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Query the database
	rows, err := db.Query("SELECT id, name FROM users;")
	if err != nil {
		t.Fatalf("Error querying data: %v", err)
	}
	defer rows.Close()

	var names = []string{"Alice", "Bob"}

	// Process query results
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		assert.True(t, contains(names, name))
		log.Printf("User: ID=%d, Name=%s", id, name)
	}

	// Add your assertions here to validate the test results
	// For example, assert the number of rows returned

	// Assert the number of rows
	var rowCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users;").Scan(&rowCount)
	if err != nil {
		t.Fatalf("Error getting row count: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("Expected 2 rows, but got %d", rowCount)
	}
}

func contains(slice []string, element string) bool {
	for _, a := range slice {
		if a == element {
			return true
		}
	}
	return false
}
