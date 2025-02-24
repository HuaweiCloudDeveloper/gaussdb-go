package pgtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/pgtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/pgxtest"
)

func TestLtreeCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type ltree")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "ltree", []pgxtest.ValueRoundTripTest{
		{
			Param:  "A.B.C",
			Result: new(string),
			Test:   isExpectedEq("A.B.C"),
		},
		{
			Param:  pgtype.Text{String: "", Valid: true},
			Result: new(pgtype.Text),
			Test:   isExpectedEq(pgtype.Text{String: "", Valid: true}),
		},
	})
}
