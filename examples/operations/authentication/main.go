package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
)

func main() {
	fmt.Println("\uF4B3 GaussDB Authentication Methods Demo")
	fmt.Println("=======================================")

	ctx := context.Background()

	if err := testConnectionStringAuth(ctx); err != nil {
		fmt.Printf("Connection string auth test failed: %v\n", err)
	}

	if err := testConfigBuilderAuth(ctx); err != nil {
		fmt.Printf("Config builder auth test failed: %v\n", err)
	}

	if err := testConcurrentConnections(ctx); err != nil {
		fmt.Printf("Concurrent connections test failed: %v\n", err)
	}

	if err := testAuthenticationMethods(ctx); err != nil {
		fmt.Printf("Authentication methods test failed: %v\n", err)
	}

	fmt.Println("\n🎉 Authentication examples completed successfully!")
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func maskPassword(connStr string) string {
	return "password=***"
}

func analyzeAuthError(err error) {
	errMsg := strings.ToLower(err.Error())

	fmt.Println("      🔍 Error analysis:")

	switch {
	case strings.Contains(errMsg, "password authentication"):
		fmt.Println("         - Authentication failure (wrong credentials)")
		fmt.Println("         - For GaussDB: Check SHA256/MD5_SHA256 authentication")
		fmt.Println("         - Verify user exists and has login privileges")
	case strings.Contains(errMsg, "database") && strings.Contains(errMsg, "does not exist"):
		fmt.Println("         - Database does not exist")
		fmt.Println("         - Use 'postgres' database for testing")
		fmt.Println("         - Create database first: CREATE DATABASE dbname;")
	case strings.Contains(errMsg, "role") && strings.Contains(errMsg, "does not exist"):
		fmt.Println("         - User/role does not exist")
		fmt.Println("         - Create user: CREATE USER username WITH PASSWORD 'password';")
		fmt.Println("         - Grant login: ALTER USER username WITH LOGIN;")
	case strings.Contains(errMsg, "connection refused") || strings.Contains(errMsg, "could not connect"):
		fmt.Println("         - Database server not accessible")
		fmt.Println("         - Check if GaussDB is running on port 5433")
		fmt.Println("         - Verify network connectivity and firewall settings")
	case strings.Contains(errMsg, "timeout"):
		fmt.Println("         - Connection timeout")
		fmt.Println("         - Server may be overloaded or network is slow")
	default:
		fmt.Printf("         - Unknown error: %v\n", err)
		fmt.Println("         - Check database server logs")
		fmt.Println("         - Verify all connection parameters")
	}
}

func testConnectionStringAuth(ctx context.Context) error {
	fmt.Println("\n1️⃣  Connection String Authentication")
	fmt.Println("-------------------------------------")

	// Get connection parameters
	host := getEnv("GAUSSDB_HOST", "localhost")
	port := getEnv("GAUSSDB_PORT", "5433")
	user := getEnv("GAUSSDB_USER", "gaussdb")
	password := getEnv("GAUSSDB_PASSWORD", "Gaussdb@123")
	database := getEnv("GAUSSDB_DATABASE", "postgres")

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		host, port, user, password, database)

	fmt.Println("🔗 Connecting with connection string...")
	fmt.Println("   Connection:", maskPassword(connStr))

	conn, err := gaussdbgo.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("   ❌ Connection failed: %v\n", err)
		analyzeAuthError(err)
		return nil
	}
	defer conn.Close(ctx)

	fmt.Println("   ✅ Connection successful!")

	// Test connection with a simple query
	var version, currentTime string
	if err := conn.QueryRow(ctx,
		"SELECT version(), current_timestamp").Scan(&version, &currentTime); err != nil {
		return err
	}

	fmt.Println("   📊 Database version:", version)
	fmt.Println("   🕐 Server time:", currentTime)

	// Check authentication method
	if strings.Contains(version, "openGauss") || strings.Contains(version, "GaussDB") {
		fmt.Println("   🔐 Using GaussDB-specific authentication")

		// Get authentication method info
		var authMethod string
		if err := conn.QueryRow(ctx,
			"SELECT setting FROM pg_settings WHERE name = 'password_encryption_type'").Scan(&authMethod); err == nil {
			fmt.Println("   🔑 Password encryption type:", authMethod)
		}
	} else {
		fmt.Println("   🔐 Using PostgreSQL-compatible authentication")
	}

	return nil
}

func testConfigBuilderAuth(ctx context.Context) error {
	fmt.Println("\n2️⃣  Config Builder Authentication")
	fmt.Println("-----------------------------------")

	// Create config
	config, err := gaussdbgo.ParseConfig("")
	if err != nil {
		return err
	}

	config.Host = getEnv("GAUSSDB_HOST", "localhost")
	config.Port = 5433 // Default port
	config.User = getEnv("GAUSSDB_USER", "gaussdb")
	config.Password = getEnv("GAUSSDB_PASSWORD", "Gaussdb@123")
	config.Database = getEnv("GAUSSDB_DATABASE", "postgres")

	fmt.Println("🔗 Connecting with Config builder...")
	fmt.Println("   Host:", config.Host)
	fmt.Println("   Port:", config.Port)
	fmt.Println("   User:", config.User)
	fmt.Println("   Database:", config.Database)

	conn, err := gaussdbgo.ConnectConfig(ctx, config)
	if err != nil {
		fmt.Printf("   ❌ Connection failed: %v\n", err)
		analyzeAuthError(err)
		return nil
	}
	defer conn.Close(ctx)

	fmt.Println("   ✅ Connection successful!")

	// Get connection info
	var currentUser, currentDB, serverAddr string
	var serverPort int
	if err := conn.QueryRow(ctx,
		"SELECT current_user, current_database(), inet_server_addr(), inet_server_port()").Scan(
		&currentUser, &currentDB, &serverAddr, &serverPort); err != nil {
		return err
	}

	fmt.Println("   👤 Connected as:", currentUser)
	fmt.Println("   🗄️  Database:", currentDB)
	fmt.Println("   🌐 Server:", serverAddr+":", serverPort)

	// Get user privileges
	rows, err := conn.Query(ctx, `
		SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolcanlogin
		FROM pg_roles WHERE rolname = current_user`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rolname    string
			rolsuper   bool
			createRole bool
			createDB   bool
			canLogin   bool
		)
		if err := rows.Scan(&rolname, &rolsuper, &createRole, &createDB, &canLogin); err != nil {
			return err
		}

		fmt.Println("   🔑 User privileges for '" + rolname + "':")
		fmt.Println("      - Superuser:", rolsuper)
		fmt.Println("      - Create roles:", createRole)
		fmt.Println("      - Create databases:", createDB)
		fmt.Println("      - Can login:", canLogin)
	}

	// Get server settings
	settingsRows, err := conn.Query(ctx, `
		SELECT name, setting, short_desc
		FROM pg_settings
		WHERE name IN ('max_connections', 'shared_buffers', 'effective_cache_size')
		ORDER BY name`)
	if err != nil {
		return err
	}
	defer settingsRows.Close()

	fmt.Println("   ⚙️  Server settings:")
	for settingsRows.Next() {
		var name, setting, desc string
		if err := settingsRows.Scan(&name, &setting, &desc); err != nil {
			return err
		}
		fmt.Printf("      - %s: %s (%s)\n", name, setting, desc)
	}

	return nil
}

func testConcurrentConnections(ctx context.Context) error {
	fmt.Println("\n3️⃣  Concurrent Connections Test")
	fmt.Println("-------------------------------")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		getEnv("GAUSSDB_HOST", "localhost"),
		getEnv("GAUSSDB_PORT", "5433"),
		getEnv("GAUSSDB_USER", "gaussdb"),
		getEnv("GAUSSDB_PASSWORD", "Gaussdb@123"),
		getEnv("GAUSSDB_DATABASE", "postgres"))

	fmt.Println("🔗 Creating 3 concurrent connections...")

	var wg sync.WaitGroup
	results := make(chan string, 3)

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := gaussdbgo.Connect(ctx, connStr)
			if err != nil {
				results <- fmt.Sprintf("   ❌ Connection %d: %v", id, err)
				return
			}
			defer conn.Close(ctx)

			var connID, pid int
			if err := conn.QueryRow(ctx,
				"SELECT $1::int, pg_backend_pid()", id).Scan(&connID, &pid); err != nil {
				results <- fmt.Sprintf("   ❌ Connection %d query failed: %v", id, err)
				return
			}

			results <- fmt.Sprintf("   ✅ Connection %d: ID=%d, PID=%d", id, connID, pid)
		}(i)
	}

	wg.Wait()
	close(results)

	fmt.Println("   📊 Connection results:")
	for res := range results {
		fmt.Println(res)
	}

	return nil
}

func testAuthenticationMethods(ctx context.Context) error {
	fmt.Println("\n4️⃣  Authentication Method Testing")
	fmt.Println("-----------------------------------")

	testCases := []struct {
		name     string
		user     string
		password string
		database string
	}{
		{
			name:     "Valid credentials",
			user:     getEnv("GAUSSDB_USER", "gaussdb"),
			password: getEnv("GAUSSDB_PASSWORD", "Gaussdb@123"),
			database: "postgres",
		},
		{
			name:     "Invalid password",
			user:     getEnv("GAUSSDB_USER", "gaussdb"),
			password: "wrong_password",
			database: "postgres",
		},
		{
			name:     "Invalid user",
			user:     "nonexistent_user",
			password: "any_password",
			database: "postgres",
		},
		{
			name:     "Invalid database",
			user:     getEnv("GAUSSDB_USER", "gaussdb"),
			password: getEnv("GAUSSDB_PASSWORD", "Gaussdb@123"),
			database: "nonexistent_db",
		},
	}

	for i, tc := range testCases {
		fmt.Printf("\n🧪 Test %d: %s\n", i+1, tc.name)
		testAuthScenario(ctx, tc.user, tc.password, tc.database)
	}

	return nil
}

func testAuthScenario(ctx context.Context, user, password, database string) {
	host := getEnv("GAUSSDB_HOST", "localhost")
	port := 5433

	fmt.Println("   🔗 Attempting connection:")
	fmt.Println("      Host:", host)
	fmt.Println("      Port:", port)
	fmt.Println("      User:", user)
	fmt.Println("      Password:", maskPassword("password="+password))
	fmt.Println("      Database:", database)

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		host, port, user, password, database)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := gaussdbgo.Connect(ctx, connStr)
	if err != nil {
		fmt.Println("      ❌ Connection failed:", err)
		analyzeAuthError(err)
		return
	}
	defer conn.Close(ctx)

	fmt.Println("      ✅ Connection successful!")

	var test int
	var timestamp time.Time
	if err := conn.QueryRow(ctx,
		"SELECT 1 as test, now() as timestamp").Scan(&test, &timestamp); err != nil {
		fmt.Println("      ❌ Test query failed:", err)
		return
	}

	fmt.Printf("      ✅ Test query result: %d, timestamp: %v\n", test, timestamp)
}
