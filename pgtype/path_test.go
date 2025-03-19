package pgtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtest"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype"
)

func isExpectedEqPath(a any) func(any) bool {
	return func(v any) bool {
		ap := a.(pgtype.Path)
		vp := v.(pgtype.Path)

		if !(ap.Valid == vp.Valid && ap.Closed == vp.Closed && len(ap.P) == len(vp.P)) {
			return false
		}

		for i := range ap.P {
			if ap.P[i] != vp.P[i] {
				return false
			}
		}

		return true
	}
}

func TestPathTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support type path")

	gaussdbtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "path", []gaussdbtest.ValueRoundTripTest{
		{
			pgtype.Path{
				P:      []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}},
				Closed: false,
				Valid:  true,
			},
			new(pgtype.Path),
			isExpectedEqPath(pgtype.Path{
				P:      []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}},
				Closed: false,
				Valid:  true,
			}),
		},
		{
			pgtype.Path{
				P:      []pgtype.Vec2{{3.14, 1.678}, {7.1, 5.234}, {23.1, 9.34}},
				Closed: true,
				Valid:  true,
			},
			new(pgtype.Path),
			isExpectedEqPath(pgtype.Path{
				P:      []pgtype.Vec2{{3.14, 1.678}, {7.1, 5.234}, {23.1, 9.34}},
				Closed: true,
				Valid:  true,
			}),
		},
		{
			pgtype.Path{
				P:      []pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Closed: true,
				Valid:  true,
			},
			new(pgtype.Path),
			isExpectedEqPath(pgtype.Path{
				P:      []pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Closed: true,
				Valid:  true,
			}),
		},
		{pgtype.Path{}, new(pgtype.Path), isExpectedEqPath(pgtype.Path{})},
		{nil, new(pgtype.Path), isExpectedEqPath(pgtype.Path{})},
	})
}
