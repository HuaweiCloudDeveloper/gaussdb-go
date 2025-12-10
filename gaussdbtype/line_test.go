package gaussdbtype_test

import (
	"context"
	"testing"

	gaussdbx "github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
)

func TestLineTranscode(t *testing.T) {
	t.Skip("GaussDB currently does not support the line data type.")
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *gaussdbx.Conn) {

		if _, ok := conn.TypeMap().TypeForName("line"); !ok {
			t.Skip("Skipping due to no line type")
		}

		// TODO: remove specific version test?
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

	gaussdbxtest.RunValueRoundTripTests(context.Background(), t, ctr, nil, "line", []gaussdbxtest.ValueRoundTripTest{
		{
			gaussdbtype.Line{
				A: 1.23, B: 4.56, C: 7.89012345,
				Valid: true,
			},
			new(gaussdbtype.Line),
			isExpectedEq(gaussdbtype.Line{
				A: 1.23, B: 4.56, C: 7.89012345,
				Valid: true,
			}),
		},
		{
			gaussdbtype.Line{
				A: -1.23, B: -4.56, C: -7.89,
				Valid: true,
			},
			new(gaussdbtype.Line),
			isExpectedEq(gaussdbtype.Line{
				A: -1.23, B: -4.56, C: -7.89,
				Valid: true,
			}),
		},
		{gaussdbtype.Line{}, new(gaussdbtype.Line), isExpectedEq(gaussdbtype.Line{})},
		{nil, new(gaussdbtype.Line), isExpectedEq(gaussdbtype.Line{})},
	})
}
