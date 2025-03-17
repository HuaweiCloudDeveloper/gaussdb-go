package zeronull_test

import (
	"context"
	"os"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/pgtype/zeronull"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner gaussdbtest.ConnTestRunner

func init() {
	defaultConnTestRunner = gaussdbtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config, err := gaussdb.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		return config
	}
	defaultConnTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		zeronull.Register(conn.TypeMap())
	}
}
