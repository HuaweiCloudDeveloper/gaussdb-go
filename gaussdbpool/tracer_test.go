package gaussdbpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testTracer struct {
	traceAcquireStart func(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireStartData) context.Context
	traceAcquireEnd   func(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData)
	traceRelease      func(pool *gaussdbpool.Pool, data gaussdbpool.TraceReleaseData)
}

type ctxKey string

func (tt *testTracer) TraceAcquireStart(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireStartData) context.Context {
	if tt.traceAcquireStart != nil {
		return tt.traceAcquireStart(ctx, pool, data)
	}
	return ctx
}

func (tt *testTracer) TraceAcquireEnd(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData) {
	if tt.traceAcquireEnd != nil {
		tt.traceAcquireEnd(ctx, pool, data)
	}
}

func (tt *testTracer) TraceRelease(pool *gaussdbpool.Pool, data gaussdbpool.TraceReleaseData) {
	if tt.traceRelease != nil {
		tt.traceRelease(pool, data)
	}
}

func (tt *testTracer) TraceQueryStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testTracer) TraceQueryEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
}

func TestTraceAcquire(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := gaussdbpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer

	pool, err := gaussdbpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	traceAcquireStartCalled := false
	tracer.traceAcquireStart = func(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireStartData) context.Context {
		traceAcquireStartCalled = true
		require.NotNil(t, pool)
		return context.WithValue(ctx, ctxKey("fromTraceAcquireStart"), "foo")
	}

	traceAcquireEndCalled := false
	tracer.traceAcquireEnd = func(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData) {
		traceAcquireEndCalled = true
		require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceAcquireStart")))
		require.NotNil(t, pool)
		require.NotNil(t, data.Conn)
		require.NoError(t, data.Err)
	}

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()
	require.True(t, traceAcquireStartCalled)
	require.True(t, traceAcquireEndCalled)

	traceAcquireStartCalled = false
	traceAcquireEndCalled = false
	tracer.traceAcquireEnd = func(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData) {
		traceAcquireEndCalled = true
		require.NotNil(t, pool)
		require.Nil(t, data.Conn)
		require.Error(t, data.Err)
	}

	ctx, cancel = context.WithCancel(ctx)
	cancel()
	_, err = pool.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, traceAcquireStartCalled)
	require.True(t, traceAcquireEndCalled)
}

func TestTraceRelease(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := gaussdbpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer

	pool, err := gaussdbpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	traceReleaseCalled := false
	tracer.traceRelease = func(pool *gaussdbpool.Pool, data gaussdbpool.TraceReleaseData) {
		traceReleaseCalled = true
		require.NotNil(t, pool)
		require.NotNil(t, data.Conn)
	}

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	c.Release()
	require.True(t, traceReleaseCalled)
}
