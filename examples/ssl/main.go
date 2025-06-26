package main

import (
	"context"
	"fmt"
	"os"

	gaussdbgo "github.com/HuaweiCloudDeveloper/gaussdb-go"
)

func main() {
	fmt.Println("\uF4B3 GaussDB SSL Demo")
	fmt.Println("=======================================")
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "gaussdb://gaussdb:Gaussdb@123@localhost:5433/postgres?sslmode=prefer"
	}

	conn, err := gaussdbgo.Connect(context.Background(), databaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	err = conn.Ping(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Ping database failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Connected successfully!")

	var greeting string
	err = conn.QueryRow(context.Background(), "SELECT 'Connect GaussDB by using sslmode=prefer'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(greeting)
}
