package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxpool"
)

func main() {
	fmt.Println("\uF4B3 GaussDB Pool Demo")
	fmt.Println("=======================================")
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "gaussdb://gaussdb:Gaussdb@123@localhost:5433/postgres"
	}

	pool, err := gaussdbxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}

	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to ping database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Successfully connected to the database!")

	_, err = pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    `)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to create table: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Successfully created 'users' table.")

	name := "Alice"
	// Ensure the email is unique to allow re-running the script.
	email := fmt.Sprintf("alice-%d@example.com", time.Now().Unix())
	_, err = pool.Exec(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", name, email)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to insert data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Successfully inserted user: %s\n", name)

	var userID int
	var userName, userEmail string
	var createdAt time.Time

	err = pool.QueryRow(ctx, "SELECT id, name, email, created_at FROM users WHERE email = $1", email).Scan(&userID, &userName, &userEmail, &createdAt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("----------- Query Result -----------")
	fmt.Printf("ID: %d\n", userID)
	fmt.Printf("Name: %s\n", userName)
	fmt.Printf("Email: %s\n", userEmail)
	fmt.Printf("Created At: %s\n", createdAt.Format(time.RFC3339))
	fmt.Println("------------------------------------")

	fmt.Println("\nDemo: Manually acquiring a connection:")
	conn, err := pool.Acquire(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to acquire a connection from the pool: %v\n", err)
		os.Exit(1)
	}
	defer conn.Release()

	var count int
	err = conn.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&count)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Query failed on the acquired connection: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Total rows in 'users' table: %d\n", count)
}
