package pgtype_test

import (
	"context"
	"math"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/gaussdbtest"
)

func TestQcharTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support qchar")

	var tests []gaussdbtest.ValueRoundTripTest
	for i := 0; i <= math.MaxUint8; i++ {
		tests = append(tests, gaussdbtest.ValueRoundTripTest{rune(i), new(rune), isExpectedEq(rune(i))})
		tests = append(tests, gaussdbtest.ValueRoundTripTest{byte(i), new(byte), isExpectedEq(byte(i))})
	}
	tests = append(tests, gaussdbtest.ValueRoundTripTest{nil, new(*rune), isExpectedEq((*rune)(nil))})
	tests = append(tests, gaussdbtest.ValueRoundTripTest{nil, new(*byte), isExpectedEq((*byte)(nil))})

	// Can only test with known OIDs as rune and byte would be considered numbers.
	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, gaussdbtest.KnownOIDQueryExecModes, `"char"`, tests)
}
