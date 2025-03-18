package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype"
)

func TestLineTranscode(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *gaussdb.Conn) {
		gaussdbtest.SkipCockroachDB(t, conn, "Server does not support type line")

		if _, ok := conn.TypeMap().TypeForName("line"); !ok {
			t.Skip("Skipping due to no line type")
		}

		// line may exist but not be usable on 9.3 :(
		var isPG93 bool
		err := conn.QueryRow(context.Background(), "select version() ~ '9.3'").Scan(&isPG93)
		if err != nil {
			t.Fatal(err)
		}
		if isPG93 {
			t.Skip("Skipping due to unimplemented line type in PG 9.3")
		}
	}

	gaussdbtest.RunValueRoundTripTests(context.Background(), t, ctr, nil, "line", []gaussdbtest.ValueRoundTripTest{
		{
			pgtype.Line{
				A: 1.23, B: 4.56, C: 7.89012345,
				Valid: true,
			},
			new(pgtype.Line),
			isExpectedEq(pgtype.Line{
				A: 1.23, B: 4.56, C: 7.89012345,
				Valid: true,
			}),
		},
		{
			pgtype.Line{
				A: -1.23, B: -4.56, C: -7.89,
				Valid: true,
			},
			new(pgtype.Line),
			isExpectedEq(pgtype.Line{
				A: -1.23, B: -4.56, C: -7.89,
				Valid: true,
			}),
		},
		{pgtype.Line{}, new(pgtype.Line), isExpectedEq(pgtype.Line{})},
		{nil, new(pgtype.Line), isExpectedEq(pgtype.Line{})},
	})
}
