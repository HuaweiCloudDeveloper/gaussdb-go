package zeronull_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/pgtype/zeronull"
)

func isExpectedEq(a any) func(any) bool {
	return func(v any) bool {
		return a == v
	}
}

func TestFloat8Transcode(t *testing.T) {
	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []gaussdbtest.ValueRoundTripTest{
		{
			(zeronull.Float8)(1),
			new(zeronull.Float8),
			isExpectedEq((zeronull.Float8)(1)),
		},
		{
			nil,
			new(zeronull.Float8),
			isExpectedEq((zeronull.Float8)(0)),
		},
		{
			(zeronull.Float8)(0),
			new(any),
			isExpectedEq(nil),
		},
	})
}
