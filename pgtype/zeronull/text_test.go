package zeronull_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype/zeronull"
)

func TestTextTranscode(t *testing.T) {
	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "text", []gaussdbtest.ValueRoundTripTest{
		{
			(zeronull.Text)("foo"),
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("foo")),
		},
		{
			nil,
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("")),
		},
		{
			(zeronull.Text)(""),
			new(any),
			isExpectedEq(nil),
		},
	})
}
