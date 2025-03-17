package gaussdb_test

import (
	"context"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1"
	gaussdbtest "github.com/HuaweiCloudDeveloper/gaussdb-go/v1/gaussdbtest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testTracer struct {
	traceQueryStart    func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context
	traceQueryEnd      func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData)
	traceBatchStart    func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context
	traceBatchQuery    func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData)
	traceBatchEnd      func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData)
	traceCopyFromStart func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context
	traceCopyFromEnd   func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData)
	tracePrepareStart  func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareStartData) context.Context
	tracePrepareEnd    func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData)
	traceConnectStart  func(ctx context.Context, data gaussdb.TraceConnectStartData) context.Context
	traceConnectEnd    func(ctx context.Context, data gaussdb.TraceConnectEndData)
}

type ctxKey string

func (tt *testTracer) TraceQueryStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
	if tt.traceQueryStart != nil {
		return tt.traceQueryStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceQueryEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
	if tt.traceQueryEnd != nil {
		tt.traceQueryEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceBatchStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
	if tt.traceBatchStart != nil {
		return tt.traceBatchStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceBatchQuery(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
	if tt.traceBatchQuery != nil {
		tt.traceBatchQuery(ctx, conn, data)
	}
}

func (tt *testTracer) TraceBatchEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
	if tt.traceBatchEnd != nil {
		tt.traceBatchEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceCopyFromStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context {
	if tt.traceCopyFromStart != nil {
		return tt.traceCopyFromStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TraceCopyFromEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData) {
	if tt.traceCopyFromEnd != nil {
		tt.traceCopyFromEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TracePrepareStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareStartData) context.Context {
	if tt.tracePrepareStart != nil {
		return tt.tracePrepareStart(ctx, conn, data)
	}
	return ctx
}

func (tt *testTracer) TracePrepareEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData) {
	if tt.tracePrepareEnd != nil {
		tt.tracePrepareEnd(ctx, conn, data)
	}
}

func (tt *testTracer) TraceConnectStart(ctx context.Context, data gaussdb.TraceConnectStartData) context.Context {
	if tt.traceConnectStart != nil {
		return tt.traceConnectStart(ctx, data)
	}
	return ctx
}

func (tt *testTracer) TraceConnectEnd(ctx context.Context, data gaussdb.TraceConnectEndData) {
	if tt.traceConnectEnd != nil {
		tt.traceConnectEnd(ctx, data)
	}
}

func TestTraceExec(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceQueryStartCalled := false
		tracer.traceQueryStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
			traceQueryStartCalled = true
			require.Equal(t, `select $1::text`, data.SQL)
			require.Len(t, data.Args, 1)
			require.Equal(t, `testing`, data.Args[0])
			return context.WithValue(ctx, ctxKey(ctxKey("fromTraceQueryStart")), "foo")
		}

		traceQueryEndCalled := false
		tracer.traceQueryEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
			traceQueryEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey(ctxKey("fromTraceQueryStart"))))
			require.Equal(t, `SELECT 1`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		_, err := conn.Exec(ctx, `select $1::text`, "testing")
		require.NoError(t, err)
		require.True(t, traceQueryStartCalled)
		require.True(t, traceQueryEndCalled)
	})
}

func TestTraceQuery(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceQueryStartCalled := false
		tracer.traceQueryStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
			traceQueryStartCalled = true
			require.Equal(t, `select $1::text`, data.SQL)
			require.Len(t, data.Args, 1)
			require.Equal(t, `testing`, data.Args[0])
			return context.WithValue(ctx, ctxKey("fromTraceQueryStart"), "foo")
		}

		traceQueryEndCalled := false
		tracer.traceQueryEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
			traceQueryEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceQueryStart")))
			require.Equal(t, `SELECT 1`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		var s string
		err := conn.QueryRow(ctx, `select $1::text`, "testing").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "testing", s)
		require.True(t, traceQueryStartCalled)
		require.True(t, traceQueryEndCalled)
	})
}

func TestTraceBatchNormal(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 2, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		batch := &gaussdb.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)

		var n int32
		err := br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		require.EqualValues(t, 1, traceBatchQueryCalledCount)

		err = br.QueryRow().Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)

		err = br.Close()
		require.NoError(t, err)

		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchClose(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 2, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.NoError(t, data.Err)
		}

		batch := &gaussdb.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)
		err := br.Close()
		require.NoError(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchErrorWhileReadingResults(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, []gaussdb.QueryExecMode{gaussdb.QueryExecModeSimpleProtocol}, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 3, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			if traceBatchQueryCalledCount == 2 {
				require.Error(t, data.Err)
			} else {
				require.NoError(t, data.Err)
			}
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.Error(t, data.Err)
		}

		batch := &gaussdb.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2/n-2 from generate_series(0,10) n`)
		batch.Queue(`select 3`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)

		commandTag, err := br.Exec()
		require.NoError(t, err)
		require.Equal(t, "SELECT 1", commandTag.String())

		commandTag, err = br.Exec()
		require.Error(t, err)
		require.Equal(t, "", commandTag.String())

		commandTag, err = br.Exec()
		require.Error(t, err)
		require.Equal(t, "", commandTag.String())

		err = br.Close()
		require.Error(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceBatchErrorWhileReadingResultsWhileClosing(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, []gaussdb.QueryExecMode{gaussdb.QueryExecModeSimpleProtocol}, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		traceBatchStartCalled := false
		tracer.traceBatchStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
			traceBatchStartCalled = true
			require.NotNil(t, data.Batch)
			require.Equal(t, 3, data.Batch.Len())
			return context.WithValue(ctx, ctxKey("fromTraceBatchStart"), "foo")
		}

		traceBatchQueryCalledCount := 0
		tracer.traceBatchQuery = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
			traceBatchQueryCalledCount++
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			if traceBatchQueryCalledCount == 2 {
				require.Error(t, data.Err)
			} else {
				require.NoError(t, data.Err)
			}
		}

		traceBatchEndCalled := false
		tracer.traceBatchEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
			traceBatchEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceBatchStart")))
			require.Error(t, data.Err)
		}

		batch := &gaussdb.Batch{}
		batch.Queue(`select 1`)
		batch.Queue(`select 2/n-2 from generate_series(0,10) n`)
		batch.Queue(`select 3`)

		br := conn.SendBatch(context.Background(), batch)
		require.True(t, traceBatchStartCalled)
		err := br.Close()
		require.Error(t, err)
		require.EqualValues(t, 2, traceBatchQueryCalledCount)
		require.True(t, traceBatchEndCalled)
	})
}

func TestTraceCopyFrom(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		traceCopyFromStartCalled := false
		tracer.traceCopyFromStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context {
			traceCopyFromStartCalled = true
			require.Equal(t, gaussdb.Identifier{"foo"}, data.TableName)
			require.Equal(t, []string{"a"}, data.ColumnNames)
			return context.WithValue(ctx, ctxKey("fromTraceCopyFromStart"), "foo")
		}

		traceCopyFromEndCalled := false
		tracer.traceCopyFromEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData) {
			traceCopyFromEndCalled = true
			require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceCopyFromStart")))
			require.Equal(t, `COPY 2`, data.CommandTag.String())
			require.NoError(t, data.Err)
		}

		_, err := conn.Exec(ctx, `create temporary table foo(a int4)`)
		require.NoError(t, err)

		inputRows := [][]any{
			{int32(1)},
			{nil},
		}

		copyCount, err := conn.CopyFrom(ctx, gaussdb.Identifier{"foo"}, []string{"a"}, gaussdb.CopyFromRows(inputRows))
		require.NoError(t, err)
		require.EqualValues(t, len(inputRows), copyCount)
		require.True(t, traceCopyFromStartCalled)
		require.True(t, traceCopyFromEndCalled)
	})
}

func TestTracePrepare(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctr := defaultConnTestRunner
	ctr.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdb.ConnConfig {
		config := defaultConnTestRunner.CreateConfig(ctx, t)
		config.Tracer = tracer
		return config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gaussdbtest.RunWithQueryExecModes(ctx, t, ctr, nil, func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		tracePrepareStartCalled := false
		tracer.tracePrepareStart = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareStartData) context.Context {
			tracePrepareStartCalled = true
			require.Equal(t, `ps`, data.Name)
			require.Equal(t, `select $1::text`, data.SQL)
			return context.WithValue(ctx, ctxKey("fromTracePrepareStart"), "foo")
		}

		tracePrepareEndCalled := false
		tracer.tracePrepareEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData) {
			tracePrepareEndCalled = true
			require.False(t, data.AlreadyPrepared)
			require.NoError(t, data.Err)
		}

		_, err := conn.Prepare(ctx, "ps", `select $1::text`)
		require.NoError(t, err)
		require.True(t, tracePrepareStartCalled)
		require.True(t, tracePrepareEndCalled)

		tracePrepareStartCalled = false
		tracePrepareEndCalled = false
		tracer.tracePrepareEnd = func(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData) {
			tracePrepareEndCalled = true
			require.True(t, data.AlreadyPrepared)
			require.NoError(t, data.Err)
		}

		_, err = conn.Prepare(ctx, "ps", `select $1::text`)
		require.NoError(t, err)
		require.True(t, tracePrepareStartCalled)
		require.True(t, tracePrepareEndCalled)
	})
}

func TestTraceConnect(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	config := defaultConnTestRunner.CreateConfig(context.Background(), t)
	config.Tracer = tracer

	traceConnectStartCalled := false
	tracer.traceConnectStart = func(ctx context.Context, data gaussdb.TraceConnectStartData) context.Context {
		traceConnectStartCalled = true
		require.NotNil(t, data.ConnConfig)
		return context.WithValue(ctx, ctxKey("fromTraceConnectStart"), "foo")
	}

	traceConnectEndCalled := false
	tracer.traceConnectEnd = func(ctx context.Context, data gaussdb.TraceConnectEndData) {
		traceConnectEndCalled = true
		require.NotNil(t, data.Conn)
		require.NoError(t, data.Err)
	}

	conn1, err := gaussdb.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer conn1.Close(context.Background())
	require.True(t, traceConnectStartCalled)
	require.True(t, traceConnectEndCalled)

	config, err = gaussdb.ParseConfig("host=/invalid")
	require.NoError(t, err)
	config.Tracer = tracer

	traceConnectStartCalled = false
	traceConnectEndCalled = false
	tracer.traceConnectEnd = func(ctx context.Context, data gaussdb.TraceConnectEndData) {
		traceConnectEndCalled = true
		require.Nil(t, data.Conn)
		require.Error(t, data.Err)
	}

	conn2, err := gaussdb.ConnectConfig(context.Background(), config)
	require.Nil(t, conn2)
	require.Error(t, err)
	require.True(t, traceConnectStartCalled)
	require.True(t, traceConnectEndCalled)
}
