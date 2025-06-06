package gaussdbconn_test

import (
	"context"
	"testing"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeConn(t testing.TB, conn *gaussdbconn.GaussdbConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
	select {
	case <-conn.CleanupDone():
	case <-time.After(30 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, gaussdbConn *gaussdbconn.GaussdbConn) {
	// in github action may timeout,so increate from 30 to 70.
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	result := gaussdbConn.ExecParams(ctx, "select generate_series(1,$1)", [][]byte{[]byte("3")}, nil, nil, nil).Read()
	cancel()

	require.Nil(t, result.Err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, "1", string(result.Rows[0][0]))
	assert.Equal(t, "2", string(result.Rows[1][0]))
	assert.Equal(t, "3", string(result.Rows[2][0]))
}
