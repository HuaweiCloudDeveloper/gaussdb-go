package gaussdbtype_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
	_ "github.com/HuaweiCloudDeveloper/gaussdb-go/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner gaussdbxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = gaussdbxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *gaussdbgo.ConnConfig {
		config, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
		require.NoError(t, err)
		return config
	}
}

// Test for renamed types
type _string string
type _bool bool
type _uint8 uint8
type _int8 int8
type _int16 int16
type _int16Slice []int16
type _int32Slice []int32
type _int64Slice []int64
type _float32Slice []float32
type _float64Slice []float64
type _byteSlice []byte

// unregisteredOID represents an actual type that is not registered. Cannot use 0 because that represents that the type
// is not known (e.g. when using the simple protocol).
const unregisteredOID = uint32(1)

func mustParseInet(t testing.TB, s string) *net.IPNet {
	ip, ipnet, err := net.ParseCIDR(s)
	if err == nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			ipnet.IP = ipv4
		} else {
			ipnet.IP = ip
		}
		return ipnet
	}

	// May be bare IP address.
	//
	ip = net.ParseIP(s)
	if ip == nil {
		t.Fatal(errors.New("unable to parse inet address"))
	}
	ipnet = &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
	if ipv4 := ip.To4(); ipv4 != nil {
		ipnet.IP = ipv4
		ipnet.Mask = net.CIDRMask(32, 32)
	}
	return ipnet
}

func mustParseMacaddr(t testing.TB, s string) net.HardwareAddr {
	addr, err := net.ParseMAC(s)
	if err != nil {
		t.Fatal(err)
	}

	return addr
}

// sqlScannerFunc lets an arbitrary function be used as a sql.Scanner.
type sqlScannerFunc func(src any) error

func (f sqlScannerFunc) Scan(src any) error {
	return f(src)
}

// driverValuerFunc lets an arbitrary function be used as a driver.Valuer.
type driverValuerFunc func() (driver.Value, error)

func (f driverValuerFunc) Value() (driver.Value, error) {
	return f()
}

func TestMapScanNilIsNoOp(t *testing.T) {
	m := gaussdbtype.NewMap()

	err := m.Scan(gaussdbtype.TextOID, gaussdbgo.TextFormatCode, []byte("foo"), nil)
	assert.NoError(t, err)
}

func TestMapScanTextFormatInterfacePtr(t *testing.T) {
	m := gaussdbtype.NewMap()
	var got any
	err := m.Scan(gaussdbtype.TextOID, gaussdbgo.TextFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestMapScanTextFormatNonByteaIntoByteSlice(t *testing.T) {
	m := gaussdbtype.NewMap()
	var got []byte
	err := m.Scan(gaussdbtype.JSONBOID, gaussdbgo.TextFormatCode, []byte("{}"), &got)
	require.NoError(t, err)
	assert.Equal(t, []byte("{}"), got)
}

func TestMapScanBinaryFormatInterfacePtr(t *testing.T) {
	m := gaussdbtype.NewMap()
	var got any
	err := m.Scan(gaussdbtype.TextOID, gaussdbgo.BinaryFormatCode, []byte("foo"), &got)
	require.NoError(t, err)
	assert.Equal(t, "foo", got)
}

func TestMapScanUnknownOIDToStringsAndBytes(t *testing.T) {
	unknownOID := uint32(999999)
	srcBuf := []byte("foo")
	m := gaussdbtype.NewMap()

	var s string
	err := m.Scan(unknownOID, gaussdbgo.TextFormatCode, srcBuf, &s)
	assert.NoError(t, err)
	assert.Equal(t, "foo", s)

	var rs _string
	err = m.Scan(unknownOID, gaussdbgo.TextFormatCode, srcBuf, &rs)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(rs))

	var b []byte
	err = m.Scan(unknownOID, gaussdbgo.TextFormatCode, srcBuf, &b)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), b)

	var rb _byteSlice
	err = m.Scan(unknownOID, gaussdbgo.TextFormatCode, srcBuf, &rb)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo"), []byte(rb))
}

func TestMapScanPointerToNilStructDoesNotCrash(t *testing.T) {
	m := gaussdbtype.NewMap()

	type myStruct struct{}
	var p *myStruct
	err := m.Scan(0, gaussdbgo.TextFormatCode, []byte("(foo,bar)"), &p)
	require.NotNil(t, err)
}

func TestMapScanUnknownOIDTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()

	var n int32
	err := m.Scan(0, gaussdbgo.TextFormatCode, []byte("123"), &n)
	assert.NoError(t, err)
	assert.EqualValues(t, 123, n)
}

func TestMapScanUnknownOIDIntoSQLScanner(t *testing.T) {
	m := gaussdbtype.NewMap()

	var s sql.NullString
	err := m.Scan(0, gaussdbgo.TextFormatCode, []byte(nil), &s)
	assert.NoError(t, err)
	assert.Equal(t, "", s.String)
	assert.False(t, s.Valid)
}

type scannerString string

func (ss *scannerString) Scan(v any) error {
	*ss = scannerString("scanned")
	return nil
}

func TestMapScanUnregisteredOIDIntoRenamedStringSQLScanner(t *testing.T) {
	m := gaussdbtype.NewMap()

	var s scannerString
	err := m.Scan(unregisteredOID, gaussdbgo.TextFormatCode, []byte(nil), &s)
	assert.NoError(t, err)
	assert.Equal(t, "scanned", string(s))
}

type gaussdbCustomInt int64

func (ci *gaussdbCustomInt) Scan(src interface{}) error {
	*ci = gaussdbCustomInt(src.(int64))
	return nil
}

func TestScanPlanBinaryInt32ScanScanner(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 42}
	var v gaussdbCustomInt

	plan := m.PlanScan(gaussdbtype.Int2OID, gaussdbtype.BinaryFormatCode, &v)
	err := plan.Scan(src, &v)
	require.NoError(t, err)
	require.EqualValues(t, 42, v)

	ptr := new(gaussdbCustomInt)
	plan = m.PlanScan(gaussdbtype.Int2OID, gaussdbtype.BinaryFormatCode, &ptr)
	err = plan.Scan(src, &ptr)
	require.NoError(t, err)
	require.EqualValues(t, 42, *ptr)

	ptr = new(gaussdbCustomInt)
	err = plan.Scan(nil, &ptr)
	require.NoError(t, err)
	assert.Nil(t, ptr)

	ptr = nil
	plan = m.PlanScan(gaussdbtype.Int2OID, gaussdbtype.BinaryFormatCode, &ptr)
	err = plan.Scan(src, &ptr)
	require.NoError(t, err)
	require.EqualValues(t, 42, *ptr)

	ptr = nil
	plan = m.PlanScan(gaussdbtype.Int2OID, gaussdbtype.BinaryFormatCode, &ptr)
	err = plan.Scan(nil, &ptr)
	require.NoError(t, err)
	assert.Nil(t, ptr)
}

func TestScanPlanInterface(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 42}
	var v interface{}
	plan := m.PlanScan(gaussdbtype.Int2OID, gaussdbtype.BinaryFormatCode, v)
	err := plan.Scan(src, v)
	assert.Error(t, err)
}

func TestPointerPointerStructScan(t *testing.T) {
	m := gaussdbtype.NewMap()
	type composite struct {
		ID int
	}

	int4Type, _ := m.TypeForOID(gaussdbtype.Int4OID)
	gaussdbType := &gaussdbtype.Type{
		Codec: &gaussdbtype.CompositeCodec{
			Fields: []gaussdbtype.CompositeCodecField{
				{
					Name: "id",
					Type: int4Type,
				},
			},
		},
		Name: "composite",
		OID:  215333,
	}
	m.RegisterType(gaussdbType)

	var c *composite
	plan := m.PlanScan(gaussdbType.OID, gaussdbtype.TextFormatCode, &c)
	err := plan.Scan([]byte("(1)"), &c)
	require.NoError(t, err)
	require.Equal(t, 1, c.ID)
}

func TestMapScanPtrToPtrToSlice(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := []byte("{foo,bar}")
	var v *[]string
	plan := m.PlanScan(gaussdbtype.TextArrayOID, gaussdbtype.TextFormatCode, &v)
	err := plan.Scan(src, &v)
	require.NoError(t, err)
	require.Equal(t, []string{"foo", "bar"}, *v)
}

func TestMapScanPtrToPtrToSliceOfStruct(t *testing.T) {
	type Team struct {
		TeamID int
		Name   string
	}

	// Have to use binary format because text format doesn't include type information.
	m := gaussdbtype.NewMap()
	src := []byte{0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0xc9, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1e, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x17, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x19, 0x0, 0x0, 0x0, 0x6, 0x74, 0x65, 0x61, 0x6d, 0x20, 0x31, 0x0, 0x0, 0x0, 0x1e, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x17, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x19, 0x0, 0x0, 0x0, 0x6, 0x74, 0x65, 0x61, 0x6d, 0x20, 0x32}
	var v *[]Team
	plan := m.PlanScan(gaussdbtype.RecordArrayOID, gaussdbtype.BinaryFormatCode, &v)
	err := plan.Scan(src, &v)
	require.NoError(t, err)
	require.Equal(t, []Team{{1, "team 1"}, {2, "team 2"}}, *v)
}

type databaseValuerString string

func (s databaseValuerString) Value() (driver.Value, error) {
	return fmt.Sprintf("%d", len(s)), nil
}

func TestMapEncodeTextFormatDatabaseValuerThatIsRenamedSimpleType(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := databaseValuerString("foo")
	buf, err := m.Encode(gaussdbtype.TextOID, gaussdbtype.TextFormatCode, src, nil)
	require.NoError(t, err)
	require.Equal(t, "3", string(buf))
}

type databaseValuerFmtStringer string

func (s databaseValuerFmtStringer) Value() (driver.Value, error) {
	return nil, nil
}

func (s databaseValuerFmtStringer) String() string {
	return "foobar"
}

func TestMapEncodeTextFormatDatabaseValuerThatIsFmtStringer(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := databaseValuerFmtStringer("")
	buf, err := m.Encode(gaussdbtype.TextOID, gaussdbtype.TextFormatCode, src, nil)
	require.NoError(t, err)
	require.Nil(t, buf)
}

type databaseValuerStringFormat struct {
	n int32
}

func (v databaseValuerStringFormat) Value() (driver.Value, error) {
	return fmt.Sprint(v.n), nil
}

func TestMapEncodeBinaryFormatDatabaseValuerThatReturnsString(t *testing.T) {
	m := gaussdbtype.NewMap()
	src := databaseValuerStringFormat{n: 42}
	buf, err := m.Encode(gaussdbtype.Int4OID, gaussdbtype.BinaryFormatCode, src, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 42}, buf)
}

func TestMapEncodeDatabaseValuerThatReturnsStringIntoUnregisteredTypeTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(unregisteredOID, gaussdbtype.TextFormatCode, driverValuerFunc(func() (driver.Value, error) { return "foo", nil }), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), buf)
}

func TestMapEncodeDatabaseValuerThatReturnsByteSliceIntoUnregisteredTypeTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(unregisteredOID, gaussdbtype.TextFormatCode, driverValuerFunc(func() (driver.Value, error) { return []byte{0, 1, 2, 3}, nil }), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`\x00010203`), buf)
}

func TestMapEncodeStringIntoUnregisteredTypeTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(unregisteredOID, gaussdbtype.TextFormatCode, "foo", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), buf)
}

func TestMapEncodeByteSliceIntoUnregisteredTypeTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(unregisteredOID, gaussdbtype.TextFormatCode, []byte{0, 1, 2, 3}, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`\x00010203`), buf)
}

func TestMapEncodeNamedTypeOfByteSliceIntoTextTextFormat(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(gaussdbtype.TextOID, gaussdbtype.TextFormatCode, json.RawMessage(`{"foo": "bar"}`), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"foo": "bar"}`), buf)
}

func TestMapScanPointerToRenamedType(t *testing.T) {
	srcBuf := []byte("foo")
	m := gaussdbtype.NewMap()

	var rs *_string
	err := m.Scan(gaussdbtype.TextOID, gaussdbgo.TextFormatCode, srcBuf, &rs)
	assert.NoError(t, err)
	require.NotNil(t, rs)
	assert.Equal(t, "foo", string(*rs))
}

func TestMapScanNullToWrongType(t *testing.T) {
	m := gaussdbtype.NewMap()

	var n *int32
	err := m.Scan(gaussdbtype.TextOID, gaussdbgo.TextFormatCode, nil, &n)
	assert.NoError(t, err)
	assert.Nil(t, n)

	var pn gaussdbtype.Int4
	err = m.Scan(gaussdbtype.TextOID, gaussdbgo.TextFormatCode, nil, &pn)
	assert.NoError(t, err)
	assert.False(t, pn.Valid)
}

func TestScanToSliceOfRenamedUint8(t *testing.T) {
	m := gaussdbtype.NewMap()
	var ruint8 []_uint8
	err := m.Scan(gaussdbtype.Int2ArrayOID, gaussdbgo.TextFormatCode, []byte("{2,4}"), &ruint8)
	assert.NoError(t, err)
	assert.Equal(t, []_uint8{2, 4}, ruint8)
}

func TestMapScanTextToBool(t *testing.T) {
	tests := []struct {
		name string
		src  []byte
		want bool
	}{
		{"t", []byte("t"), true},
		{"f", []byte("f"), false},
		{"y", []byte("y"), true},
		{"n", []byte("n"), false},
		{"1", []byte("1"), true},
		{"0", []byte("0"), false},
		{"true", []byte("true"), true},
		{"false", []byte("false"), false},
		{"yes", []byte("yes"), true},
		{"no", []byte("no"), false},
		{"on", []byte("on"), true},
		{"off", []byte("off"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gaussdbtype.NewMap()

			var v bool
			err := m.Scan(gaussdbtype.BoolOID, gaussdbgo.TextFormatCode, tt.src, &v)
			require.NoError(t, err)
			assert.Equal(t, tt.want, v)
		})
	}
}

func TestMapScanTextToBoolError(t *testing.T) {
	tests := []struct {
		name string
		src  []byte
		want string
	}{
		{"nil", nil, "cannot scan NULL into *bool"},
		{"empty", []byte{}, "cannot scan empty string into *bool"},
		{"foo", []byte("foo"), "unknown boolean string representation \"foo\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gaussdbtype.NewMap()

			var v bool
			err := m.Scan(gaussdbtype.BoolOID, gaussdbgo.TextFormatCode, tt.src, &v)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

type databaseValuerUUID [16]byte

func (v databaseValuerUUID) Value() (driver.Value, error) {
	return fmt.Sprintf("%x", v), nil
}

func TestMapEncodePlanCacheUUIDTypeConfusion(t *testing.T) {
	expected := []byte{
		0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xb, 0x86, 0, 0, 0, 2, 0, 0, 0, 1,
		0, 0, 0, 16,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		0, 0, 0, 16,
		15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

	m := gaussdbtype.NewMap()
	buf, err := m.Encode(gaussdbtype.UUIDArrayOID, gaussdbtype.BinaryFormatCode,
		[]databaseValuerUUID{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}},
		nil)
	require.NoError(t, err)
	require.Equal(t, expected, buf)

	// This actually *should* fail. In the actual query path this error is detected and the encoding falls back to the
	// text format. In the bug this test is guarding against regression this would panic.
	_, err = m.Encode(gaussdbtype.UUIDArrayOID, gaussdbtype.BinaryFormatCode,
		[]string{"00010203-0405-0607-0809-0a0b0c0d0e0f", "0f0e0d0c-0b0a-0908-0706-0504-03020100"},
		nil)
	require.Error(t, err)
}

func TestMapEncodeRawJSONIntoUnknownOID(t *testing.T) {
	m := gaussdbtype.NewMap()
	buf, err := m.Encode(0, gaussdbtype.TextFormatCode, json.RawMessage(`{"foo": "bar"}`), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"foo": "bar"}`), buf)
}

func BenchmarkMapScanInt4IntoBinaryDecoder(b *testing.B) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v gaussdbtype.Int4

	for i := 0; i < b.N; i++ {
		v = gaussdbtype.Int4{}
		err := m.Scan(gaussdbtype.Int4OID, gaussdbtype.BinaryFormatCode, src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != (gaussdbtype.Int4{Int32: 42, Valid: true}) {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkMapScanInt4IntoGoInt32(b *testing.B) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v int32

	for i := 0; i < b.N; i++ {
		v = 0
		err := m.Scan(gaussdbtype.Int4OID, gaussdbtype.BinaryFormatCode, src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != 42 {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkScanPlanScanInt4IntoBinaryDecoder(b *testing.B) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v gaussdbtype.Int4

	plan := m.PlanScan(gaussdbtype.Int4OID, gaussdbtype.BinaryFormatCode, &v)

	for i := 0; i < b.N; i++ {
		v = gaussdbtype.Int4{}
		err := plan.Scan(src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != (gaussdbtype.Int4{Int32: 42, Valid: true}) {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func BenchmarkScanPlanScanInt4IntoGoInt32(b *testing.B) {
	m := gaussdbtype.NewMap()
	src := []byte{0, 0, 0, 42}
	var v int32

	plan := m.PlanScan(gaussdbtype.Int4OID, gaussdbtype.BinaryFormatCode, &v)

	for i := 0; i < b.N; i++ {
		v = 0
		err := plan.Scan(src, &v)
		if err != nil {
			b.Fatal(err)
		}
		if v != 42 {
			b.Fatal("scan failed due to bad value")
		}
	}
}

func isExpectedEq(a any) func(any) bool {
	return func(v any) bool {
		return a == v
	}
}

func isPtrExpectedEq(a any) func(any) bool {
	return func(v any) bool {
		val := reflect.ValueOf(v)
		return a == val.Elem().Interface()
	}
}
