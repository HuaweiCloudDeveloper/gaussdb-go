package gaussdbtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
)

func TestFloat8Codec(t *testing.T) {
	gaussdbxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []gaussdbxtest.ValueRoundTripTest{
		{gaussdbtype.Float8{Float64: -1, Valid: true}, new(gaussdbtype.Float8), isExpectedEq(gaussdbtype.Float8{Float64: -1, Valid: true})},
		{gaussdbtype.Float8{Float64: 0, Valid: true}, new(gaussdbtype.Float8), isExpectedEq(gaussdbtype.Float8{Float64: 0, Valid: true})},
		{gaussdbtype.Float8{Float64: 1, Valid: true}, new(gaussdbtype.Float8), isExpectedEq(gaussdbtype.Float8{Float64: 1, Valid: true})},
		{float64(0.00001), new(float64), isExpectedEq(float64(0.00001))},
		{float64(9999.99), new(float64), isExpectedEq(float64(9999.99))},
		{gaussdbtype.Float8{}, new(gaussdbtype.Float8), isExpectedEq(gaussdbtype.Float8{})},
		{int64(1), new(int64), isExpectedEq(int64(1))},
		// todo: same as TestFloat4Codec
		//{"1.23", new(string), isExpectedEq("1.23")},
		{nil, new(*float64), isExpectedEq((*float64)(nil))},
	})
}

func TestFloat8MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source gaussdbtype.Float8
		result string
	}{
		{source: gaussdbtype.Float8{Float64: 0}, result: "null"},
		{source: gaussdbtype.Float8{Float64: 1.23, Valid: true}, result: "1.23"},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestFloat8UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result gaussdbtype.Float8
	}{
		{source: "null", result: gaussdbtype.Float8{Float64: 0}},
		{source: "1.23", result: gaussdbtype.Float8{Float64: 1.23, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r gaussdbtype.Float8
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
