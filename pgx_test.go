package gaussdb_test

import (
	"context"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1"
	"os"
	"testing"

	_ "github.com/HuaweiCloudDeveloper/gaussdb-go/v1/stdlib"
)

func skipCockroachDB(t testing.TB, msg string) {
	conn, err := gaussdb.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip(msg)
	}
}
