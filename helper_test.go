package gaussdbgo_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner gaussdbxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = gaussdbxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdbgo.ConnConfig {
		config, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
		require.NoError(t, err)
		return config
	}
}

func mustConnectString(t testing.TB, connString string) *gaussdbgo.Conn {
	conn, err := gaussdbgo.Connect(context.Background(), connString)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

func mustParseConfig(t testing.TB, connString string) *gaussdbgo.ConnConfig {
	config, err := gaussdbgo.ParseConfig(connString)
	require.Nil(t, err)
	return config
}

func mustConnect(t testing.TB, config *gaussdbgo.ConnConfig) *gaussdbgo.Conn {
	conn, err := gaussdbgo.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

func closeConn(t testing.TB, conn *gaussdbgo.Conn) {
	err := conn.Close(context.Background())
	if err != nil {
		t.Fatalf("conn.Close unexpectedly failed: %v", err)
	}
}

func mustExec(t testing.TB, conn *gaussdbgo.Conn, sql string, arguments ...any) (commandTag gaussdbconn.CommandTag) {
	var err error
	if commandTag, err = conn.Exec(context.Background(), sql, arguments...); err != nil {
		t.Fatalf("Exec unexpectedly failed with %v: %v", sql, err)
	}
	return
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t testing.TB, conn *gaussdbgo.Conn) {
	var sum, rowCount int32

	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", rows.Err())
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

func assertConfigsEqual(t *testing.T, expected, actual *gaussdbgo.ConnConfig, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Tracer, actual.Tracer, "%s - Tracer", testName)
	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)
	assert.Equalf(t, expected.StatementCacheCapacity, actual.StatementCacheCapacity, "%s - StatementCacheCapacity", testName)
	assert.Equalf(t, expected.DescriptionCacheCapacity, actual.DescriptionCacheCapacity, "%s - DescriptionCacheCapacity", testName)
	assert.Equalf(t, expected.DefaultQueryExecMode, actual.DefaultQueryExecMode, "%s - DefaultQueryExecMode", testName)
	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}

func lowerFirstLetterInError(s string) string {
	if !gaussdbgo.IsTestingWithOpengauss() || s == "" || strings.HasPrefix(s, "SQLSTATE") {
		return s
	}
	prefix := ""
	leftOver := s
	if strings.HasPrefix(s, "ERROR: ") {
		prefix = "ERROR: "
		leftOver = strings.TrimPrefix(s, prefix)
	}
	return prefix + strings.ToLower(leftOver[:1]) + leftOver[1:]
}
