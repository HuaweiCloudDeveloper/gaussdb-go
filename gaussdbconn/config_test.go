package gaussdbconn_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	gaussdbgo "github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func skipOnWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("FIXME: skipping on Windows, investigate why this test fails in CI environment")
	}
}

func getDefaultPort(t *testing.T) uint16 {
	if envGaussdbPORT := os.Getenv("GAUSSDB_PORT"); envGaussdbPORT != "" {
		p, err := strconv.ParseUint(envGaussdbPORT, 10, 16)
		require.NoError(t, err)
		return uint16(p)
	}
	return 5432
}

func getDefaultUser(t *testing.T) string {
	if gaussdbUser := os.Getenv("GAUSSDB_USER"); gaussdbUser != "" {
		return gaussdbUser
	}

	var osUserName string
	osUser, err := user.Current()
	if err == nil {
		// Windows gives us the username here as `DOMAIN\user` or `LOCALPCNAME\user`,
		// but the libpq default is just the `user` portion, so we strip off the first part.
		if runtime.GOOS == "windows" && strings.Contains(osUser.Username, "\\") {
			osUserName = osUser.Username[strings.LastIndex(osUser.Username, "\\")+1:]
		} else {
			osUserName = osUser.Username
		}
	}

	return osUserName
}

func TestParseConfig(t *testing.T) {
	skipOnWindows(t)
	t.Parallel()

	config, err := gaussdbconn.ParseConfig("")
	require.NoError(t, err)
	defaultHost := config.Host

	defaultUser := getDefaultUser(t)
	defaultPort := getDefaultPort(t)

	tests := []struct {
		name       string
		connString string
		config     *gaussdbconn.Config
	}{
		// Test all sslmodes
		{
			name:       "sslmode not set (prefer)",
			connString: "gaussdb://jack:secret@localhost:5432/mydb",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "localhost",
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "localhost",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "sslmode disable",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode allow",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=allow",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host: "localhost",
						Port: 5432,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
							ServerName:         "localhost",
						},
					},
				},
			},
		},
		{
			name:       "sslmode prefer",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=prefer",
			config: &gaussdbconn.Config{

				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "localhost",
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "localhost",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "sslmode require",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=require",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "localhost",
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode verify-ca",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=verify-ca",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "localhost",
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode verify-full",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=verify-full",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     &tls.Config{ServerName: "localhost"},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url everything",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&application_name=gaussdbxtest&search_path=myschema&connect_timeout=5",
			config: &gaussdbconn.Config{
				User:           "jack",
				Password:       "secret",
				Host:           "localhost",
				Port:           5432,
				Database:       "mydb",
				TLSConfig:      nil,
				ConnectTimeout: 5 * time.Second,
				RuntimeParams: map[string]string{
					"application_name": "gaussdbxtest",
					"search_path":      "myschema",
				},
			},
		},
		{
			name:       "database url missing password",
			connString: "gaussdb://jack@localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url missing user and password",
			connString: "gaussdb://localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          defaultUser,
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url missing port",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url unix domain socket host",
			connString: "gaussdb:///foo?host=/tmp",
			config: &gaussdbconn.Config{
				User:          defaultUser,
				Host:          "/tmp",
				Port:          defaultPort,
				Database:      "foo",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url unix domain socket host on windows",
			connString: "gaussdb:///foo?host=C:\\tmp",
			config: &gaussdbconn.Config{
				User:          defaultUser,
				Host:          "C:\\tmp",
				Port:          defaultPort,
				Database:      "foo",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url dbname",
			connString: "gaussdb://localhost/?dbname=foo&sslmode=disable",
			config: &gaussdbconn.Config{
				User:          defaultUser,
				Host:          "localhost",
				Port:          defaultPort,
				Database:      "foo",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url gaussdb protocol",
			connString: "gaussdb://jack@localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv4 with port",
			connString: "gaussdb://jack@127.0.0.1:5433/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "127.0.0.1",
				Port:          5433,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv6 with port",
			connString: "gaussdb://jack@[2001:db8::1]:5433/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "2001:db8::1",
				Port:          5433,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv6 no port",
			connString: "gaussdb://jack@[2001:db8::1]/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "2001:db8::1",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value everything",
			connString: "user=jack password=secret host=localhost port=5432 dbname=mydb sslmode=disable application_name=gaussdbxtest search_path=myschema connect_timeout=5",
			config: &gaussdbconn.Config{
				User:           "jack",
				Password:       "secret",
				Host:           "localhost",
				Port:           5432,
				Database:       "mydb",
				TLSConfig:      nil,
				ConnectTimeout: 5 * time.Second,
				RuntimeParams: map[string]string{
					"application_name": "gaussdbxtest",
					"search_path":      "myschema",
				},
			},
		},
		{
			name:       "Key/value with escaped single quote",
			connString: "user=jack\\'s password=secret host=localhost port=5432 dbname=mydb sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack's",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value with escaped backslash",
			connString: "user=jack password=sooper\\\\secret host=localhost port=5432 dbname=mydb sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "sooper\\secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value with single quoted values",
			connString: "user='jack' host='localhost' dbname='mydb' sslmode='disable'",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value with single quoted value with escaped single quote",
			connString: "user='jack\\'s' host='localhost' dbname='mydb' sslmode='disable'",
			config: &gaussdbconn.Config{
				User:          "jack's",
				Host:          "localhost",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value with empty single quoted value",
			connString: "user='jack' password='' host='localhost' dbname='mydb' sslmode='disable'",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value with space between key and value",
			connString: "user = 'jack' password = '' host = 'localhost' dbname = 'mydb' sslmode='disable'",
			config: &gaussdbconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "URL multiple hosts",
			connString: "gaussdb://jack:secret@foo,bar,baz/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          defaultPort,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "bar",
						Port:      defaultPort,
						TLSConfig: nil,
					},
					{
						Host:      "baz",
						Port:      defaultPort,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "URL multiple hosts and ports",
			connString: "gaussdb://jack:secret@foo:1,bar:2,baz:3/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "bar",
						Port:      2,
						TLSConfig: nil,
					},
					{
						Host:      "baz",
						Port:      3,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "URL without host but with port still uses default host",
			connString: "gaussdb://jack:secret@:1/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          defaultHost,
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "Key/value multiple hosts one port",
			connString: "user=jack password=secret host=foo,bar,baz port=5432 dbname=mydb sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "bar",
						Port:      5432,
						TLSConfig: nil,
					},
					{
						Host:      "baz",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "Key/value multiple hosts multiple ports",
			connString: "user=jack password=secret host=foo,bar,baz port=1,2,3 dbname=mydb sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "bar",
						Port:      2,
						TLSConfig: nil,
					},
					{
						Host:      "baz",
						Port:      3,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "multiple hosts and fallback tls",
			connString: "user=jack password=secret host=foo,bar,baz dbname=mydb sslmode=prefer",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "foo",
				Port:     defaultPort,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "foo",
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "foo",
						Port:      defaultPort,
						TLSConfig: nil,
					},
					{
						Host: "bar",
						Port: defaultPort,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
							ServerName:         "bar",
						}},
					{
						Host:      "bar",
						Port:      defaultPort,
						TLSConfig: nil,
					},
					{
						Host: "baz",
						Port: defaultPort,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
							ServerName:         "baz",
						}},
					{
						Host:      "baz",
						Port:      defaultPort,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "target_session_attrs read-write",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=read-write",
			config: &gaussdbconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: gaussdbconn.ValidateConnectTargetSessionAttrsReadWrite,
			},
		},
		{
			name:       "target_session_attrs read-only",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=read-only",
			config: &gaussdbconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: gaussdbconn.ValidateConnectTargetSessionAttrsReadOnly,
			},
		},
		{
			name:       "target_session_attrs primary",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=primary",
			config: &gaussdbconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: gaussdbconn.ValidateConnectTargetSessionAttrsPrimary,
			},
		},
		{
			name:       "target_session_attrs standby",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=standby",
			config: &gaussdbconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: gaussdbconn.ValidateConnectTargetSessionAttrsStandby,
			},
		},
		{
			name:       "target_session_attrs prefer-standby",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=prefer-standby",
			config: &gaussdbconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: gaussdbconn.ValidateConnectTargetSessionAttrsPreferStandby,
			},
		},
		{
			name:       "target_session_attrs any",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=any",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "target_session_attrs not set (any)",
			connString: "gaussdb://jack:secret@localhost:5432/mydb?sslmode=disable",
			config: &gaussdbconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "SNI is set by default",
			connString: "gaussdb://jack:secret@sni.test:5432/mydb?sslmode=require",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "sni.test",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "sni.test",
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "SNI is not set for IPv4",
			connString: "gaussdb://jack:secret@1.1.1.1:5432/mydb?sslmode=require",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "1.1.1.1",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "SNI is not set for IPv6",
			connString: "gaussdb://jack:secret@[::1]:5432/mydb?sslmode=require",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "::1",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "SNI is not set when disabled (URL-style)",
			connString: "gaussdb://jack:secret@sni.test:5432/mydb?sslmode=require&sslsni=0",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "sni.test",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "SNI is not set when disabled (key/value style)",
			connString: "user=jack password=secret host=sni.test dbname=mydb sslmode=require sslsni=0",
			config: &gaussdbconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "sni.test",
				Port:     defaultPort,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
	}

	for i, tt := range tests {
		config, err := gaussdbconn.ParseConfig(tt.connString)
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func TestParseConfigKVWithTrailingEmptyEqualDoesNotPanic(t *testing.T) {
	_, err := gaussdbconn.ParseConfig("host= user= password= port= database=")
	require.NoError(t, err)
}

func TestParseConfigKVLeadingEqual(t *testing.T) {
	_, err := gaussdbconn.ParseConfig("= user=jack")
	require.Error(t, err)
}

func TestParseConfigKVTrailingBackslash(t *testing.T) {
	_, err := gaussdbconn.ParseConfig(`x=x\`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid backslash")
}

func TestConfigCopyReturnsEqualConfig(t *testing.T) {
	connString := "gaussdb://jack:secret@localhost:5432/mydb?application_name=gaussdbxtest&search_path=myschema&connect_timeout=5"
	original, err := gaussdbconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")
}

func TestConfigCopyOriginalConfigDidNotChange(t *testing.T) {
	connString := "gaussdb://jack:secret@localhost:5432/mydb?application_name=gaussdbxtest&search_path=myschema&connect_timeout=5&sslmode=prefer"
	original, err := gaussdbconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")

	copied.Port = uint16(5433)
	copied.RuntimeParams["foo"] = "bar"
	copied.Fallbacks[0].Port = uint16(5433)

	assert.Equal(t, uint16(5432), original.Port)
	assert.Equal(t, "", original.RuntimeParams["foo"])
	assert.Equal(t, uint16(5432), original.Fallbacks[0].Port)
}

func TestConfigCopyCanBeUsedToConnect(t *testing.T) {
	connString := os.Getenv(gaussdbgo.EnvGaussdbTestDatabase)
	original, err := gaussdbconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assert.NotPanics(t, func() {
		_, err = gaussdbconn.ConnectConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
}

func TestNetworkAddress(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantNet string
	}{
		{
			name:    "Default Unix socket address",
			host:    "/var/run/gaussdb",
			wantNet: "unix",
		},
		{
			name:    "Windows Unix socket address (standard drive name)",
			host:    "C:\\tmp",
			wantNet: "unix",
		},
		{
			name:    "Windows Unix socket address (first drive name)",
			host:    "A:\\tmp",
			wantNet: "unix",
		},
		{
			name:    "Windows Unix socket address (last drive name)",
			host:    "Z:\\tmp",
			wantNet: "unix",
		},
		{
			name:    "Assume TCP for unknown formats",
			host:    "a/tmp",
			wantNet: "tcp",
		},
		{
			name:    "loopback interface",
			host:    "localhost",
			wantNet: "tcp",
		},
		{
			name:    "IP address",
			host:    "127.0.0.1",
			wantNet: "tcp",
		},
	}
	for i, tt := range tests {
		gotNet, _ := gaussdbconn.NetworkAddress(tt.host, 5432)

		assert.Equalf(t, tt.wantNet, gotNet, "Test %d (%s)", i, tt.name)
	}
}

func assertConfigsEqual(t *testing.T, expected, actual *gaussdbconn.Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}

func TestParseConfigEnvLibpq(t *testing.T) {
	var osUserName string
	osUser, err := user.Current()
	if err == nil {
		// Windows gives us the username here as `DOMAIN\user` or `LOCALPCNAME\user`,
		// but the libpq default is just the `user` portion, so we strip off the first part.
		if runtime.GOOS == "windows" && strings.Contains(osUser.Username, "\\") {
			osUserName = osUser.Username[strings.LastIndex(osUser.Username, "\\")+1:]
		} else {
			osUserName = osUser.Username
		}
	}
	// TODO: What are the corresponding environment variable names for these in GaussDB?
	gaussdbEnvVars := []string{"GAUSSDB_HOST", "GAUSSDB_PORT", "GAUSSDB_DATABASE", "GAUSSDB_USER", "GAUSSDB_PASSWORD",
		"GAUSSDB_APPNAME", "GAUSSDB_SSLMODE", "GAUSSDB_CONNECT_TIMEOUT", "GAUSSDB_SSLSNI"}

	tests := []struct {
		name    string
		envVars map[string]string
		config  *gaussdbconn.Config
	}{
		{
			// not testing no environment at all as that would use default host and that can vary.
			name:    "GAUSSDB_HOST only",
			envVars: map[string]string{"GAUSSDB_HOST": "123.123.123.123"},
			config: &gaussdbconn.Config{
				User: osUserName,
				Host: "123.123.123.123",
				Port: 5432,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "123.123.123.123",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name: "All non-TLS environment",
			envVars: map[string]string{
				"GAUSSDB_HOST":            "123.123.123.123",
				"GAUSSDB_PORT":            "7777",
				"GAUSSDB_DATABASE":        "foo",
				"GAUSSDB_USER":            "bar",
				"GAUSSDB_PASSWORD":        "baz",
				"GAUSSDB_CONNECT_TIMEOUT": "10",
				"GAUSSDB_SSLMODE":         "disable",
				"GAUSSDB_APPNAME":         "gaussdbxtest",
			},
			config: &gaussdbconn.Config{
				Host:           "123.123.123.123",
				Port:           7777,
				Database:       "foo",
				User:           "bar",
				Password:       "baz",
				ConnectTimeout: 10 * time.Second,
				TLSConfig:      nil,
				RuntimeParams:  map[string]string{"application_name": "gaussdbxtest"},
			},
		},
		{
			name: "SNI can be disabled via environment variable",
			envVars: map[string]string{
				"GAUSSDB_HOST":    "test.foo",
				"GAUSSDB_SSLMODE": "require",
				"GAUSSDB_SSLSNI":  "0",
			},
			config: &gaussdbconn.Config{
				User: osUserName,
				Host: "test.foo",
				Port: 5432,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
	}

	for i, tt := range tests {
		for _, env := range gaussdbEnvVars {
			t.Setenv(env, tt.envVars[env])
		}

		config, err := gaussdbconn.ParseConfig("")
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func TestParseConfigReadsGaussdbPassfile(t *testing.T) {
	skipOnWindows(t)
	t.Parallel()

	tfName := filepath.Join(t.TempDir(), "config")
	err := os.WriteFile(tfName, []byte("test1:5432:curlydb:curly:nyuknyuknyuk"), 0600)
	require.NoError(t, err)

	connString := fmt.Sprintf("gaussdb://curly@test1:5432/curlydb?sslmode=disable&passfile=%s", tfName)
	expected := &gaussdbconn.Config{
		User:          "curly",
		Password:      "nyuknyuknyuk",
		Host:          "test1",
		Port:          5432,
		Database:      "curlydb",
		TLSConfig:     nil,
		RuntimeParams: map[string]string{},
	}

	actual, err := gaussdbconn.ParseConfig(connString)
	assert.NoError(t, err)

	assertConfigsEqual(t, expected, actual, "passfile")
}

func TestParseConfigReadsGaussdbServiceFile(t *testing.T) {
	skipOnWindows(t)
	t.Parallel()

	tfName := filepath.Join(t.TempDir(), "config")

	err := os.WriteFile(tfName, []byte(`
[abc]
host=abc.example.com
port=9999
dbname=abcdb
user=abcuser

[def]
host = def.example.com
dbname = defdb
user = defuser
application_name = spaced string
`), 0600)
	require.NoError(t, err)

	defaultPort := getDefaultPort(t)

	tests := []struct {
		name       string
		connString string
		config     *gaussdbconn.Config
	}{
		{
			name:       "abc",
			connString: fmt.Sprintf("gaussdb:///?servicefile=%s&service=%s", tfName, "abc"),
			config: &gaussdbconn.Config{
				Host:     "abc.example.com",
				Database: "abcdb",
				User:     "abcuser",
				Port:     9999,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "abc.example.com",
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "abc.example.com",
						Port:      9999,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "def",
			connString: fmt.Sprintf("gaussdb:///?servicefile=%s&service=%s", tfName, "def"),
			config: &gaussdbconn.Config{
				Host:     "def.example.com",
				Port:     defaultPort,
				Database: "defdb",
				User:     "defuser",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         "def.example.com",
				},
				RuntimeParams: map[string]string{"application_name": "spaced string"},
				Fallbacks: []*gaussdbconn.FallbackConfig{
					{
						Host:      "def.example.com",
						Port:      defaultPort,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "conn string has precedence",
			connString: fmt.Sprintf("gaussdb://other.example.com:7777/?servicefile=%s&service=%s&sslmode=disable", tfName, "abc"),
			config: &gaussdbconn.Config{
				Host:          "other.example.com",
				Database:      "abcdb",
				User:          "abcuser",
				Port:          7777,
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
	}

	for i, tt := range tests {
		config, err := gaussdbconn.ParseConfig(tt.connString)
		if !assert.NoErrorf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}
