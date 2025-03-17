// package gaussdbtest provides utilities for testing pgx and packages that integrate with gaussdb.
package gaussdbtest

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1"
)

var AllQueryExecModes = []gaussdb.QueryExecMode{
	gaussdb.QueryExecModeCacheStatement,
	gaussdb.QueryExecModeCacheDescribe,
	gaussdb.QueryExecModeDescribeExec,
	gaussdb.QueryExecModeExec,
	gaussdb.QueryExecModeSimpleProtocol,
}

// KnownOIDQueryExecModes is a slice of all query exec modes where the param and result OIDs are known before sending the query.
var KnownOIDQueryExecModes = []gaussdb.QueryExecMode{
	gaussdb.QueryExecModeCacheStatement,
	gaussdb.QueryExecModeCacheDescribe,
	gaussdb.QueryExecModeDescribeExec,
}

// ConnTestRunner controls how a *gaussdb.Conn is created and closed by tests. All fields are required. Use DefaultConnTestRunner to get a
// ConnTestRunner with reasonable default values.
type ConnTestRunner struct {
	// CreateConfig returns a *gaussdb.ConnConfig suitable for use with gaussdb.ConnectConfig.
	CreateConfig func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig

	// AfterConnect is called after conn is established. It allows for arbitrary connection setup before a test begins.
	AfterConnect func(ctx context.Context, t testing.TB, conn *gaussdb.Conn)

	// AfterTest is called after the test is run. It allows for validating the state of the connection before it is closed.
	AfterTest func(ctx context.Context, t testing.TB, conn *gaussdb.Conn)

	// CloseConn closes conn.
	CloseConn func(ctx context.Context, t testing.TB, conn *gaussdb.Conn)
}

// DefaultConnTestRunner returns a new ConnTestRunner with all fields set to reasonable default values.
func DefaultConnTestRunner() ConnTestRunner {
	return ConnTestRunner{
		CreateConfig: func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
			config, err := gaussdb.ParseConfig("")
			if err != nil {
				t.Fatalf("ParseConfig failed: %v", err)
			}
			return config
		},
		AfterConnect: func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {},
		AfterTest:    func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {},
		CloseConn: func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
			err := conn.Close(ctx)
			if err != nil {
				t.Errorf("Close failed: %v", err)
			}
		},
	}
}

func (ctr *ConnTestRunner) RunTest(ctx context.Context, t testing.TB, f func(ctx context.Context, t testing.TB, conn *gaussdb.Conn)) {
	t.Helper()

	config := ctr.CreateConfig(ctx, t)
	conn, err := gaussdb.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("ConnectConfig failed: %v", err)
	}
	defer ctr.CloseConn(ctx, t, conn)

	ctr.AfterConnect(ctx, t, conn)
	f(ctx, t, conn)
	ctr.AfterTest(ctx, t, conn)
}

// RunWithQueryExecModes runs a f in a new test for each element of modes with a new connection created using connector.
// If modes is nil all gaussdb.QueryExecModes are tested.
func RunWithQueryExecModes(ctx context.Context, t *testing.T, ctr ConnTestRunner, modes []gaussdb.QueryExecMode, f func(ctx context.Context, t testing.TB, conn *gaussdb.Conn)) {
	if modes == nil {
		modes = AllQueryExecModes
	}

	for _, mode := range modes {
		ctrWithMode := ctr
		ctrWithMode.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
			config := ctr.CreateConfig(ctx, t)
			config.DefaultQueryExecMode = mode
			return config
		}

		t.Run(mode.String(),
			func(t *testing.T) {
				ctrWithMode.RunTest(ctx, t, f)
			},
		)
	}
}

type ValueRoundTripTest struct {
	Param  any
	Result any
	Test   func(any) bool
}

func RunValueRoundTripTests(
	ctx context.Context,
	t testing.TB,
	ctr ConnTestRunner,
	modes []gaussdb.QueryExecMode,
	pgTypeName string,
	tests []ValueRoundTripTest,
) {
	t.Helper()

	if modes == nil {
		modes = AllQueryExecModes
	}

	ctr.RunTest(ctx, t, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		t.Helper()

		sql := fmt.Sprintf("select $1::%s", pgTypeName)

		for i, tt := range tests {
			for _, mode := range modes {
				err := conn.QueryRow(ctx, sql, mode, tt.Param).Scan(tt.Result)
				if err != nil {
					t.Errorf("%d. %v: %v", i, mode, err)
				}

				result := reflect.ValueOf(tt.Result)
				if result.Kind() == reflect.Ptr {
					result = result.Elem()
				}

				if !tt.Test(result.Interface()) {
					t.Errorf("%d. %v: unexpected result for %v: %v", i, mode, tt.Param, result.Interface())
				}
			}
		}
	})
}

// SkipCockroachDB calls Skip on t with msg if the connection is to a CockroachDB server.
func SkipCockroachDB(t testing.TB, conn *gaussdb.Conn, msg string) {
	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip(msg)
	}
}

func SkipPostgreSQLVersionLessThan(t testing.TB, conn *gaussdb.Conn, minVersion int64) {
	serverVersionStr := conn.PgConn().ParameterStatus("server_version")
	serverVersionStr = regexp.MustCompile(`^[0-9]+`).FindString(serverVersionStr)
	// if not PostgreSQL do nothing
	if serverVersionStr == "" {
		return
	}

	serverVersion, err := strconv.ParseInt(serverVersionStr, 10, 64)
	if err != nil {
		t.Fatalf("postgres version parsed failed: %s", err)
	}

	if serverVersion < minVersion {
		t.Skipf("Test requires PostgreSQL v%d+", minVersion)
	}
}
