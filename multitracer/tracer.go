// Package multitracer provides a Tracer that can combine several tracers into one.
package multitracer

import (
	"context"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/gaussdbpool"
)

// Tracer can combine several tracers into one.
// You can use New to automatically split tracers by interface.
type Tracer struct {
	QueryTracers       []gaussdb.QueryTracer
	BatchTracers       []gaussdb.BatchTracer
	CopyFromTracers    []gaussdb.CopyFromTracer
	PrepareTracers     []gaussdb.PrepareTracer
	ConnectTracers     []gaussdb.ConnectTracer
	PoolAcquireTracers []gaussdbpool.AcquireTracer
	PoolReleaseTracers []gaussdbpool.ReleaseTracer
}

// New returns new Tracer from tracers with automatically split tracers by interface.
func New(tracers ...gaussdb.QueryTracer) *Tracer {
	var t Tracer

	for _, tracer := range tracers {
		t.QueryTracers = append(t.QueryTracers, tracer)

		if batchTracer, ok := tracer.(gaussdb.BatchTracer); ok {
			t.BatchTracers = append(t.BatchTracers, batchTracer)
		}

		if copyFromTracer, ok := tracer.(gaussdb.CopyFromTracer); ok {
			t.CopyFromTracers = append(t.CopyFromTracers, copyFromTracer)
		}

		if prepareTracer, ok := tracer.(gaussdb.PrepareTracer); ok {
			t.PrepareTracers = append(t.PrepareTracers, prepareTracer)
		}

		if connectTracer, ok := tracer.(gaussdb.ConnectTracer); ok {
			t.ConnectTracers = append(t.ConnectTracers, connectTracer)
		}

		if poolAcquireTracer, ok := tracer.(gaussdbpool.AcquireTracer); ok {
			t.PoolAcquireTracers = append(t.PoolAcquireTracers, poolAcquireTracer)
		}

		if poolReleaseTracer, ok := tracer.(gaussdbpool.ReleaseTracer); ok {
			t.PoolReleaseTracers = append(t.PoolReleaseTracers, poolReleaseTracer)
		}
	}

	return &t
}

func (t *Tracer) TraceQueryStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryStartData) context.Context {
	for _, tracer := range t.QueryTracers {
		ctx = tracer.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceQueryEndData) {
	for _, tracer := range t.QueryTracers {
		tracer.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchStartData) context.Context {
	for _, tracer := range t.BatchTracers {
		ctx = tracer.TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchQueryData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceBatchEndData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromStartData) context.Context {
	for _, tracer := range t.CopyFromTracers {
		ctx = tracer.TraceCopyFromStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceCopyFromEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TraceCopyFromEndData) {
	for _, tracer := range t.CopyFromTracers {
		tracer.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *Tracer) TracePrepareStart(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareStartData) context.Context {
	for _, tracer := range t.PrepareTracers {
		ctx = tracer.TracePrepareStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TracePrepareEnd(ctx context.Context, conn *gaussdb.Conn, data gaussdb.TracePrepareEndData) {
	for _, tracer := range t.PrepareTracers {
		tracer.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceConnectStart(ctx context.Context, data gaussdb.TraceConnectStartData) context.Context {
	for _, tracer := range t.ConnectTracers {
		ctx = tracer.TraceConnectStart(ctx, data)
	}

	return ctx
}

func (t *Tracer) TraceConnectEnd(ctx context.Context, data gaussdb.TraceConnectEndData) {
	for _, tracer := range t.ConnectTracers {
		tracer.TraceConnectEnd(ctx, data)
	}
}

func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireStartData) context.Context {
	for _, tracer := range t.PoolAcquireTracers {
		ctx = tracer.TraceAcquireStart(ctx, pool, data)
	}

	return ctx
}

func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *gaussdbpool.Pool, data gaussdbpool.TraceAcquireEndData) {
	for _, tracer := range t.PoolAcquireTracers {
		tracer.TraceAcquireEnd(ctx, pool, data)
	}
}

func (t *Tracer) TraceRelease(pool *gaussdbpool.Pool, data gaussdbpool.TraceReleaseData) {
	for _, tracer := range t.PoolReleaseTracers {
		tracer.TraceRelease(pool, data)
	}
}
