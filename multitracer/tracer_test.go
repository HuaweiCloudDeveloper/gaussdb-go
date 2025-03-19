package multitracer_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbpool"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/multitracer"
	"github.com/stretchr/testify/require"
)

type testFullTracer struct{}

func (tt *testFullTracer) TraceQueryStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceQueryEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
}

func (tt *testFullTracer) TraceBatchStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceBatchQuery(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
}

func (tt *testFullTracer) TraceBatchEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
}

func (tt *testFullTracer) TraceCopyFromStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceCopyFromEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData) {
}

func (tt *testFullTracer) TracePrepareStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TracePrepareEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData) {
}

func (tt *testFullTracer) TraceConnectStart(ctx context.Context, data gaussdb.TraceConnectStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceConnectEnd(ctx context.Context, data gaussdb.TraceConnectEndData) {
}

func (tt *testFullTracer) TraceAcquireStart(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceAcquireEnd(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData) {
}

func (tt *testFullTracer) TraceRelease(pool *gaussdbpool.Pool, data gaussdbpool.TraceReleaseData) {
}

type testCopyTracer struct{}

func (tt *testCopyTracer) TraceQueryStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceQueryEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
}

func (tt *testCopyTracer) TraceCopyFromStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceCopyFromEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData) {
}

func TestNew(t *testing.T) {
	t.Parallel()

	fullTracer := &testFullTracer{}
	copyTracer := &testCopyTracer{}

	mt := multitracer.New(fullTracer, copyTracer)
	require.Equal(
		t,
		&multitracer.Tracer{
			QueryTracers: []gaussdb.QueryTracer{
				fullTracer,
				copyTracer,
			},
			BatchTracers: []gaussdb.BatchTracer{
				fullTracer,
			},
			CopyFromTracers: []gaussdb.CopyFromTracer{
				fullTracer,
				copyTracer,
			},
			PrepareTracers: []gaussdb.PrepareTracer{
				fullTracer,
			},
			ConnectTracers: []gaussdb.ConnectTracer{
				fullTracer,
			},
			PoolAcquireTracers: []gaussdbpool.AcquireTracer{
				fullTracer,
			},
			PoolReleaseTracers: []gaussdbpool.ReleaseTracer{
				fullTracer,
			},
		},
		mt,
	)
}
