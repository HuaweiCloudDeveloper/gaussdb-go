package pgtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype"
)

func TestTIDCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type tid")

	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "tid", []gaussdbtest.ValueRoundTripTest{
		{
			pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			new(pgtype.TID),
			isExpectedEq(pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true}),
		},
		{
			pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			new(pgtype.TID),
			isExpectedEq(pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true}),
		},
		{
			pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			new(string),
			isExpectedEq("(42,43)"),
		},
		{
			pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			new(string),
			isExpectedEq("(4294967295,65535)"),
		},
		{pgtype.TID{}, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
		{nil, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
	})
}
