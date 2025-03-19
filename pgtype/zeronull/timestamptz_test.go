package zeronull_test

import (
	"context"
	"testing"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype/zeronull"
)

func isExpectedEqTimestamptz(a any) func(any) bool {
	return func(v any) bool {
		at := time.Time(a.(zeronull.Timestamptz))
		vt := time.Time(v.(zeronull.Timestamptz))

		return at.Equal(vt)
	}
}

func TestTimestamptzTranscode(t *testing.T) {
	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamptz", []gaussdbtest.ValueRoundTripTest{
		{
			(zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
			new(zeronull.Timestamptz),
			isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			nil,
			new(zeronull.Timestamptz),
			isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Time{})),
		},
		{
			(zeronull.Timestamptz)(time.Time{}),
			new(any),
			isExpectedEq(nil),
		},
	})
}
