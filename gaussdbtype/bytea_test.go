package gaussdbtype_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	gaussdbx "github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
	"github.com/stretchr/testify/require"
)

func isExpectedEqBytes(a any) func(any) bool {
	return func(v any) bool {
		ab := a.([]byte)
		vb := v.([]byte)

		if (ab == nil) != (vb == nil) {
			return false
		}

		if ab == nil {
			return true
		}

		return bytes.Equal(ab, vb)
	}
}

func TestByteaCodec(t *testing.T) {
	gaussdbxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "bytea", []gaussdbxtest.ValueRoundTripTest{
		{[]byte{1, 2, 3}, new([]byte), isExpectedEqBytes([]byte{1, 2, 3})},
		{[]byte{}, new([]byte), isExpectedEqBytes([]byte{})},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})
}

func TestDriverBytesQueryRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {
		var buf []byte
		err := conn.QueryRow(ctx, `select $1::bytea`, []byte{1, 2}).Scan((*gaussdbtype.DriverBytes)(&buf))
		require.EqualError(t, err, "cannot scan into *gaussdbtype.DriverBytes from QueryRow")
	})
}

func TestDriverBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {
		argBuf := make([]byte, 128)
		for i := range argBuf {
			argBuf[i] = byte(i)
		}

		rows, err := conn.Query(ctx, `select $1::bytea from generate_series(1, 1000)`, argBuf)
		require.NoError(t, err)
		defer rows.Close()

		rowCount := 0
		resultBuf := argBuf
		detectedResultMutation := false
		for rows.Next() {
			rowCount++

			// At some point the buffer should be reused and change.
			if !bytes.Equal(argBuf, resultBuf) {
				detectedResultMutation = true
			}

			err = rows.Scan((*gaussdbtype.DriverBytes)(&resultBuf))
			require.NoError(t, err)

			require.Len(t, resultBuf, len(argBuf))
			require.Equal(t, resultBuf, argBuf)
			require.Equalf(t, cap(resultBuf), len(resultBuf), "cap(resultBuf) is larger than len(resultBuf)")
		}

		require.True(t, detectedResultMutation)

		err = rows.Err()
		require.NoError(t, err)
	})
}

func TestPreallocBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {
		origBuf := []byte{5, 6, 7, 8}
		buf := origBuf
		err := conn.QueryRow(ctx, `select $1::bytea`, []byte{1, 2}).Scan((*gaussdbtype.PreallocBytes)(&buf))
		require.NoError(t, err)

		require.Len(t, buf, 2)
		require.Equal(t, 4, cap(buf))
		require.Equal(t, []byte{1, 2}, buf)

		require.Equal(t, []byte{1, 2, 7, 8}, origBuf)

		err = conn.QueryRow(ctx, `select $1::bytea`, []byte{3, 4, 5, 6, 7}).Scan((*gaussdbtype.PreallocBytes)(&buf))
		require.NoError(t, err)
		require.Len(t, buf, 5)
		require.Equal(t, 5, cap(buf))

		require.Equal(t, []byte{1, 2, 7, 8}, origBuf)
	})
}

func TestUndecodedBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {
		var buf []byte
		err := conn.QueryRow(ctx, `select 1::int4`).Scan((*gaussdbtype.UndecodedBytes)(&buf))
		require.NoError(t, err)

		require.Len(t, buf, 4)
		require.Equal(t, []byte{0, 0, 0, 1}, buf)
	})
}

func TestByteaCodecDecodeDatabaseSQLValue(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {
		var buf []byte
		err := conn.QueryRow(ctx, `select '\xa1b2c3d4'::bytea`).Scan(sqlScannerFunc(func(src any) error {
			switch src := src.(type) {
			case []byte:
				buf = make([]byte, len(src))
				copy(buf, src)
				return nil
			default:
				return fmt.Errorf("expected []byte, got %T", src)
			}
		}))
		require.NoError(t, err)

		require.Len(t, buf, 4)
		require.Equal(t, []byte{0xa1, 0xb2, 0xc3, 0xd4}, buf)
	})
}
