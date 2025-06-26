package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
)

type Product struct {
	ID        int32
	Name      string
	Price     float64
	CreatedAt time.Time
}

type productCopySource struct {
	idx      int
	products []Product
}

func (pcs *productCopySource) Next() bool {
	pcs.idx++
	return pcs.idx < len(pcs.products)
}

func (pcs *productCopySource) Values() ([]interface{}, error) {
	p := pcs.products[pcs.idx]
	return []interface{}{p.ID, p.Name, p.Price, p.CreatedAt}, nil
}

func (pcs *productCopySource) Err() error {
	return nil
}

func main() {
	fmt.Println("\uF4B3 GaussDB COPY Demo")
	fmt.Println("=======================================")
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "gaussdb://gaussdb:Gaussdb@123@localhost:5433/postgres"
	}

	// 2. Connect to the database.
	ctx := context.Background()
	conn, err := gaussdbgo.Connect(ctx, connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	fmt.Println("✅ Successfully connected to GaussDB!")

	// 3. Create a temporary table for the demo.
	tableName := "products_temp_copy"
	fmt.Printf("\n🏗️  Creating temporary table: %s\n", tableName)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT,
			name TEXT,
			price NUMERIC(10, 2),
			created_at TIMESTAMPTZ
		);
	`, tableName, tableName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create table: %v\n", err)
		os.Exit(1)
	}
	// Use defer to ensure the table is dropped when the program exits.
	defer func() {
		fmt.Printf("\n🗑️  Cleaning up and dropping table: %s\n", tableName)
		_, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE %s;", tableName))
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to drop table: %v\n", err)
		}
	}()

	// 4. Prepare the data to be imported.
	productsToCopy := []Product{
		{ID: 1, Name: "Laptop", Price: 1299.99, CreatedAt: time.Now()},
		{ID: 2, Name: "Mechanical Keyboard", Price: 150.50, CreatedAt: time.Now()},
		{ID: 3, Name: "4K Monitor", Price: 799.00, CreatedAt: time.Now()},
		{ID: 4, Name: "Wireless Mouse", Price: 75.25, CreatedAt: time.Now()},
	}

	// 5. Execute the COPY operation.
	fmt.Println("Starting COPY operation...")

	// Define the table name and columns.
	tableIdentifier := gaussdbgo.Identifier{tableName}
	columns := []string{"id", "name", "price", "created_at"}

	// Create our CopyFromSource.
	source := &productCopySource{products: productsToCopy}

	// Call CopyFrom.
	copyCount, err := conn.CopyFrom(ctx, tableIdentifier, columns, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ COPY operation failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ COPY operation successful! Copied %d records.\n", copyCount)

	// 6. Verify that the data was inserted.
	var count int64
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to verify data: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ Verification: The table now contains %d records.\n", count)
}
