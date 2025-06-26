package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxpool"
)

func main() {
	fmt.Println("\uF4B3 Transaction Management Demo (gaussdb-go)")
	fmt.Println("============================================")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "gaussdb://gaussdb:Gaussdb@123@localhost:5433/postgres"
	}

	ctx := context.Background()
	pool, err := gaussdbxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("Unable to connect: %v", err)
	}
	defer pool.Close()
	fmt.Println("✅ Connected to GaussDB")

	setupTestTables(ctx, pool)
	basicTransactionExample(ctx, pool)
	rollbackTransactionExample(ctx, pool)
	savepointExample(ctx, pool)
	isolationLevelExample(ctx, pool)
	batchTransactionExample(ctx, pool)
	cleanupTestTables(ctx, pool)

	fmt.Println("\n🎉 Transaction examples completed successfully!")
}

func setupTestTables(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n🏗️  Setting up test tables...")
	_, err := db.Exec(ctx, `
		DROP TABLE IF EXISTS transactions CASCADE;
		DROP TABLE IF EXISTS accounts CASCADE;
		CREATE TABLE accounts (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			balance NUMERIC(10,2) NOT NULL DEFAULT 0.00,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE transactions (
			id SERIAL PRIMARY KEY,
			from_account_id INTEGER REFERENCES accounts(id),
			to_account_id INTEGER REFERENCES accounts(id),
			amount NUMERIC(10,2) NOT NULL,
			description TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		log.Fatalf("Error creating tables: %v", err)
	}
	initialData := []struct {
		name    string
		balance float64
	}{
		{"Alice", 1000},
		{"Bob", 500},
		{"Charlie", 750},
	}
	for _, acc := range initialData {
		_, err := db.Exec(ctx, `INSERT INTO accounts (name, balance) VALUES ($1, $2)`, acc.name, acc.balance)
		if err != nil {
			log.Fatalf("Error inserting initial data: %v", err)
		}
	}
	fmt.Println("   ✅ Test tables created and populated")
}

func cleanupTestTables(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n🗑️  Cleaning up test tables...")
	_, err := db.Exec(ctx, `
		DROP TABLE IF EXISTS transactions CASCADE;
		DROP TABLE IF EXISTS accounts CASCADE;
	`)
	if err != nil {
		log.Fatalf("Error dropping tables: %v", err)
	}
	fmt.Println("   ✅ Test tables dropped")
}

func showAccountBalances(ctx context.Context, db *gaussdbxpool.Pool, title string) {
	fmt.Printf("   📊 %s:\n", title)
	rows, err := db.Query(ctx, `SELECT name, balance FROM accounts ORDER BY name`)
	if err != nil {
		log.Fatalf("Error fetching balances: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var balance float64
		err = rows.Scan(&name, &balance)
		if err != nil {
			log.Fatalf("Scan error: %v", err)
		}
		fmt.Printf("      - %s: $%.2f\n", name, balance)
	}
}

func basicTransactionExample(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n1️⃣  Basic Transaction Example")
	fmt.Println("----------------------------")
	showAccountBalances(ctx, db, "Initial balances")

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	amount := 100.0
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE name = $2`, amount, "Alice")
	if err != nil {
		log.Fatalf("Debit error: %v", err)
	}
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE name = $2`, amount, "Bob")
	if err != nil {
		log.Fatalf("Credit error: %v", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO transactions (from_account_id, to_account_id, amount, description)
		VALUES ((SELECT id FROM accounts WHERE name = $1), (SELECT id FROM accounts WHERE name = $2), $3, $4)`,
		"Alice", "Bob", amount, "Basic transfer example")
	if err != nil {
		log.Fatalf("Insert transaction failed: %v", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalf("Commit failed: %v", err)
	}
	fmt.Println("   ✅ Transaction committed successfully")
	showAccountBalances(ctx, db, "After basic transaction")
}

func rollbackTransactionExample(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n2️⃣  Rollback Transaction Example")
	fmt.Println("--------------------------------")
	showAccountBalances(ctx, db, "Before rollback example")

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	amount := 200.0
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE name = $2`, amount, "Bob")
	if err != nil {
		log.Fatalf("Debit error: %v", err)
	}
	cmdTag, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE name = $2`, amount, "NonExistentUser")
	if err != nil || cmdTag.RowsAffected() == 0 {
		fmt.Println("   ❌ Error or no recipient found, rolling back...")
		tx.Rollback(ctx)
		fmt.Println("   ✅ Transaction rolled back successfully")
		showAccountBalances(ctx, db, "After rollback (unchanged)")
		return
	}
	tx.Commit(ctx)
}

func savepointExample(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n3️⃣  Savepoint Example")
	fmt.Println("--------------------")
	showAccountBalances(ctx, db, "Before savepoint example")

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	amount1 := 50.0
	amount2 := 75.0
	_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE name = $2`, amount1, "Alice")
	_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE name = $2`, amount1, "Charlie")
	tx.Exec(ctx, "SAVEPOINT sp1")
	_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE name = $2`, amount2, "Bob")
	_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE name = $2`, amount2, "Charlie")

	tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")

	tx.Commit(ctx)
	fmt.Println("   ✅ Main transaction committed")
	showAccountBalances(ctx, db, "After savepoint example")
}

func isolationLevelExample(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n4️⃣  Isolation Level Example")
	fmt.Println("---------------------------")

	levels := map[string]string{
		"READ UNCOMMITTED": "READ UNCOMMITTED",
		"READ COMMITTED":   "READ COMMITTED",
		"REPEATABLE READ":  "REPEATABLE READ",
		"SERIALIZABLE":     "SERIALIZABLE",
	}

	for name, level := range levels {
		fmt.Printf("   🔒 Testing %s isolation level\n", name)
		tx, err := db.BeginTx(ctx, gaussdbgo.TxOptions{IsoLevel: gaussdbgo.TxIsoLevel(level)})
		if err != nil {
			log.Fatalf("BeginTx failed: %v", err)
		}
		var balance float64
		err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE name = $1`, "Alice").Scan(&balance)
		if err != nil {
			log.Fatalf("QueryRow failed: %v", err)
		}
		fmt.Printf("      Alice's balance: $%.2f\n", balance)
		tx.Commit(ctx)
	}
}

func batchTransactionExample(ctx context.Context, db *gaussdbxpool.Pool) {
	fmt.Println("\n5️⃣  Batch Transaction Example")
	fmt.Println("----------------------------")
	showAccountBalances(ctx, db, "Before batch operations")

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	ops := []struct {
		from, to, desc string
		amt            float64
	}{
		{"Alice", "Bob", "Batch transfer 1", 25.00},
		{"Bob", "Charlie", "Batch transfer 2", 30.00},
		{"Charlie", "Alice", "Batch transfer 3", 15.00},
	}

	for _, op := range ops {
		_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE name = $2`, op.amt, op.from)
		_, _ = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE name = $2`, op.amt, op.to)
		_, _ = tx.Exec(ctx, `INSERT INTO transactions (from_account_id, to_account_id, amount, description) VALUES ((SELECT id FROM accounts WHERE name = $1), (SELECT id FROM accounts WHERE name = $2), $3, $4)`, op.from, op.to, op.amt, op.desc)
		fmt.Printf("   ✅ Batch operation: %s -> %s ($%.2f)\n", op.from, op.to, op.amt)
	}
	tx.Commit(ctx)
	fmt.Println("   ✅ Batch transaction committed")
	showAccountBalances(ctx, db, "After batch operations")

	fmt.Println("   📜 Transaction history:")
	rows, _ := db.Query(ctx, `SELECT t.amount, t.description, t.created_at, a1.name, a2.name FROM transactions t JOIN accounts a1 ON t.from_account_id = a1.id JOIN accounts a2 ON t.to_account_id = a2.id ORDER BY t.created_at DESC LIMIT 5`)
	defer rows.Close()
	for rows.Next() {
		var amt float64
		var desc, from, to string
		var ts time.Time
		_ = rows.Scan(&amt, &desc, &ts, &from, &to)
		fmt.Printf("      - %s -> %s: $%.2f (%s) at %s\n", from, to, amt, desc, ts.Format("15:04:05"))
	}
}
