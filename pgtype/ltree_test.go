package pgtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype"
)

func TestLtreeCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type ltree")

	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, gaussdbtest.KnownOIDQueryExecModes, "ltree", []gaussdbtest.ValueRoundTripTest{
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
