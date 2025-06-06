package gaussdbgo_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
	"github.com/stretchr/testify/require"
)

func TestConnCopyWithAllQueryExecModes(t *testing.T) {
	for _, mode := range gaussdbxtest.AllQueryExecModes {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			cfg := mustParseConfig(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
			cfg.DefaultQueryExecMode = mode
			conn := mustConnect(t, cfg)
			defer closeConn(t, conn)

			mustExec(t, conn, `create temporary table foo(
			a int2,
			b int4,
			c int8,
			d text,
			e timestamptz
		)`)

			tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

			inputRows := [][]any{
				{int16(0), int32(1), int64(2), "abc", tzedTime},
				{nil, nil, nil, nil, nil},
			}

			copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e"}, gaussdbgo.CopyFromRows(inputRows))
			if err != nil {
				t.Errorf("Unexpected error for CopyFrom: %v", err)
			}
			if int(copyCount) != len(inputRows) {
				t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
			}

			_, offset := tzedTime.Zone()
			offsetHours := offset / 3600
			tzOffset := fmt.Sprintf("%+d", offsetHours)
			mustExec(t, conn, fmt.Sprintf("SET TIME ZONE '%s'", tzOffset))

			rows, err := conn.Query(ctx, "select * from foo")
			if err != nil {
				t.Errorf("Unexpected error for Query: %v", err)
			}

			var outputRows [][]any
			for rows.Next() {
				row, err := rows.Values()
				if err != nil {
					t.Errorf("Unexpected error for rows.Values(): %v", err)
				}
				outputRows = append(outputRows, row)
			}

			if rows.Err() != nil {
				t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
			}

			if !reflect.DeepEqual(inputRows, outputRows) {
				t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
			}

			ensureConnValid(t, conn)
		})
	}
}

func TestConnCopyWithKnownOIDQueryExecModes(t *testing.T) {

	for _, mode := range gaussdbxtest.KnownOIDQueryExecModes {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			cfg := mustParseConfig(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
			cfg.DefaultQueryExecMode = mode
			conn := mustConnect(t, cfg)
			defer closeConn(t, conn)

			mustExec(t, conn, `create temporary table foo(
			a int2,
			b int4,
			c int8,
			d varchar,
			e text,
			f date,
			g timestamptz
		)`)

			tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

			inputRows := [][]any{
				{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
				{nil, nil, nil, nil, nil, nil, nil},
			}

			copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, gaussdbgo.CopyFromRows(inputRows))
			if err != nil {
				t.Errorf("Unexpected error for CopyFrom: %v", err)
			}
			if int(copyCount) != len(inputRows) {
				t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
			}

			rows, err := conn.Query(ctx, "select * from foo")
			if err != nil {
				t.Errorf("Unexpected error for Query: %v", err)
			}

			var outputRows [][]any
			for rows.Next() {
				row, err := rows.Values()
				if err != nil {
					t.Errorf("Unexpected error for rows.Values(): %v", err)
				}
				outputRows = append(outputRows, row)
			}

			if rows.Err() != nil {
				t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
			}

			if !reflect.DeepEqual(inputRows, outputRows) {
				t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
			}

			ensureConnValid(t, conn)
		})
	}
}

func TestConnCopyFromSmall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, gaussdbgo.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromSliceSmall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"},
		gaussdbgo.CopyFromSlice(len(inputRows), func(i int) ([]any, error) {
			return inputRows[i], nil
		}))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromLarge(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g timestamptz,
		h bytea
	)`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{}

	for i := 0; i < 10000; i++ {
		inputRows = append(inputRows, []any{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime, []byte{111, 111, 111, 111}})
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, gaussdbgo.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal")
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromEnum(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `drop type if exists color`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `drop type if exists fruit`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `create type color as enum ('blue', 'green', 'orange')`)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `create type fruit as enum ('apple', 'orange', 'grape')`)
	require.NoError(t, err)

	// Obviously using conn while a tx is in use and registering a type after the connection has been established are
	// really bad practices, but for the sake of convenience we do it in the test here.
	for _, name := range []string{"fruit", "color"} {
		typ, err := conn.LoadType(ctx, name)
		require.NoError(t, err)
		conn.TypeMap().RegisterType(typ)
	}

	_, err = tx.Exec(ctx, `create temporary table foo(
		a text,
		b color,
		c fruit,
		d color,
		e fruit,
		f text
	)`)
	require.NoError(t, err)

	inputRows := [][]any{
		{"abc", "blue", "grape", "orange", "orange", "def"},
		{nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := tx.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f"}, gaussdbgo.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	rows, err := tx.Query(ctx, "select * from foo")
	require.NoError(t, err)

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		require.NoError(t, err)
		outputRows = append(outputRows, row)
	}

	require.NoError(t, rows.Err())

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	ensureConnValid(t, conn)
}

func TestConnCopyFromJSON(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	for _, typeName := range []string{"json", "jsonb"} {
		if _, ok := conn.TypeMap().TypeForName(typeName); !ok {
			return // No JSON/JSONB type -- must be running against old GaussDB
		}
	}

	mustExec(t, conn, `create temporary table foo(
		a json,
		b jsonb
	)`)

	inputRows := [][]any{
		{map[string]any{"foo": "bar"}, map[string]any{"bar": "quz"}},
		{nil, nil},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b"}, gaussdbgo.CopyFromRows(inputRows))
	if err != nil {
		t.Errorf("Unexpected error for CopyFrom: %v", err)
	}
	if int(copyCount) != len(inputRows) {
		t.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if !reflect.DeepEqual(inputRows, outputRows) {
		t.Errorf("Input rows and output rows do not equal: %v -> %v", inputRows, outputRows)
	}

	ensureConnValid(t, conn)
}

type clientFailSource struct {
	count int
	err   error
}

func (cfs *clientFailSource) Next() bool {
	cfs.count++
	return cfs.count < 100
}

func (cfs *clientFailSource) Values() ([]any, error) {
	if cfs.count == 3 {
		cfs.err = fmt.Errorf("client error")
		return nil, cfs.err
	}
	return []any{make([]byte, 100000)}, nil
}

func (cfs *clientFailSource) Err() error {
	return cfs.err
}

func TestConnCopyFromFailServerSideMidway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int4,
		b varchar not null
	)`)

	inputRows := [][]any{
		{int32(1), "abc"},
		{int32(2), nil}, // this row should trigger a failure
		{int32(3), "def"},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b"}, gaussdbgo.CopyFromRows(inputRows))
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*gaussdbconn.GaussdbError); !ok {
		t.Errorf("Expected CopyFrom return gaussdbgo.GaussdbError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	mustExec(t, conn, "truncate foo")

	ensureConnValid(t, conn)
}

type failSource struct {
	count int
}

func (fs *failSource) Next() bool {
	time.Sleep(time.Millisecond * 100)
	fs.count++
	return fs.count < 100
}

func (fs *failSource) Values() ([]any, error) {
	if fs.count == 3 {
		return []any{nil}, nil
	}
	return []any{make([]byte, 100000)}, nil
}

func (fs *failSource) Err() error {
	return nil
}

func TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	startTime := time.Now()

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a"}, &failSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*gaussdbconn.GaussdbError); !ok {
		t.Errorf("Expected CopyFrom return gaussdbgo.GaussdbError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	endTime := time.Now()
	copyTime := endTime.Sub(startTime)
	if copyTime > time.Second {
		t.Errorf("Failing CopyFrom shouldn't have taken so long: %v", copyTime)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

type slowFailRaceSource struct {
	count int
}

func (fs *slowFailRaceSource) Next() bool {
	time.Sleep(time.Millisecond)
	fs.count++
	return fs.count < 1000
}

func (fs *slowFailRaceSource) Values() ([]any, error) {
	if fs.count == 500 {
		return []any{nil, nil}, nil
	}
	return []any{1, make([]byte, 1000)}, nil
}

func (fs *slowFailRaceSource) Err() error {
	return nil
}

func TestConnCopyFromSlowFailRace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int not null,
		b bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a", "b"}, &slowFailRaceSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if _, ok := err.(*gaussdbconn.GaussdbError); !ok {
		t.Errorf("Expected CopyFrom return gaussdbgo.GaussdbError, but instead it returned: %v", err)
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromCopyFromSourceErrorMidway(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a"}, &clientFailSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", len(outputRows))
	}

	ensureConnValid(t, conn)
}

type clientFinalErrSource struct {
	count int
}

func (cfs *clientFinalErrSource) Next() bool {
	cfs.count++
	return cfs.count < 5
}

func (cfs *clientFinalErrSource) Values() ([]any, error) {
	return []any{make([]byte, 100000)}, nil
}

func (cfs *clientFinalErrSource) Err() error {
	return fmt.Errorf("final error")
}

func TestConnCopyFromCopyFromSourceErrorEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a bytea not null
	)`)

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a"}, &clientFinalErrSource{})
	if err == nil {
		t.Errorf("Expected CopyFrom return error, but it did not")
	}
	if copyCount != 0 {
		t.Errorf("Expected CopyFrom to return 0 copied rows, but got %d", copyCount)
	}

	rows, err := conn.Query(ctx, "select * from foo")
	if err != nil {
		t.Errorf("Unexpected error for Query: %v", err)
	}

	var outputRows [][]any
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	if rows.Err() != nil {
		t.Errorf("Unexpected error for rows.Err(): %v", rows.Err())
	}

	if len(outputRows) != 0 {
		t.Errorf("Expected 0 rows, but got %v", outputRows)
	}

	ensureConnValid(t, conn)
}

func TestConnCopyFromAutomaticStringConversion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int8
	)`)

	inputRows := [][]interface{}{
		{"42"},
		{"7"},
		{8},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a"}, gaussdbgo.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	rows, _ := conn.Query(ctx, "select * from foo")
	nums, err := gaussdbgo.CollectRows(rows, gaussdbgo.RowTo[int64])
	require.NoError(t, err)

	require.Equal(t, []int64{42, 7, 8}, nums)

	ensureConnValid(t, conn)
}

func TestConnCopyFromAutomaticStringConversionArray(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a numeric[]
	)`)

	inputRows := [][]interface{}{
		{[]string{"42"}},
		{[]string{"7"}},
		{[]string{"8", "9"}},
		{[][]string{{"10", "11"}, {"12", "13"}}},
	}

	copyCount, err := conn.CopyFrom(ctx, gaussdbgo.Identifier{"foo"}, []string{"a"}, gaussdbgo.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.EqualValues(t, len(inputRows), copyCount)

	// Test reads as int64 and flattened array for simplicity.
	rows, _ := conn.Query(ctx, "select * from foo")
	nums, err := gaussdbgo.CollectRows(rows, gaussdbgo.RowTo[[]int64])
	require.NoError(t, err)
	require.Equal(t, [][]int64{{42}, {7}, {8, 9}, {10, 11, 12, 13}}, nums)

	ensureConnValid(t, conn)
}

func TestCopyFromFunc(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table foo(
		a int
	)`)

	dataCh := make(chan int, 1)

	const channelItems = 10
	go func() {
		for i := 0; i < channelItems; i++ {
			dataCh <- i
		}
		close(dataCh)
	}()

	copyCount, err := conn.CopyFrom(context.Background(), gaussdbgo.Identifier{"foo"}, []string{"a"},
		gaussdbgo.CopyFromFunc(func() ([]any, error) {
			v, ok := <-dataCh
			if !ok {
				return nil, nil
			}
			return []any{v}, nil
		}))

	require.ErrorIs(t, err, nil)
	require.EqualValues(t, channelItems, copyCount)

	rows, err := conn.Query(context.Background(), "select * from foo order by a")
	require.NoError(t, err)
	nums, err := gaussdbgo.CollectRows(rows, gaussdbgo.RowTo[int64])
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nums)

	// simulate a failure
	copyCount, err = conn.CopyFrom(context.Background(), gaussdbgo.Identifier{"foo"}, []string{"a"},
		gaussdbgo.CopyFromFunc(func() func() ([]any, error) {
			x := 9
			return func() ([]any, error) {
				x++
				if x > 100 {
					return nil, fmt.Errorf("simulated error")
				}
				return []any{x}, nil
			}
		}()))
	require.NotErrorIs(t, err, nil)
	require.EqualValues(t, 0, copyCount) // no change, due to error

	ensureConnValid(t, conn)
}
