package pgproto3

import (
	"encoding/json"
	"errors"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/v1/internal/pgio"
)

type ReadBuf []byte

// AuthenticationSHA256 is a message sent from the backend indicating that SASL SHA256 authentication is required.
type AuthenticationSHA256 struct {
	AuthMechanisms []string
	r              *ReadBuf
}

// Backend identifies this message as sendable by the GaussDB backend.
func (*AuthenticationSHA256) Backend() {}

// Backend identifies this message as an authentication response.
func (*AuthenticationSHA256) AuthenticationResponse() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *AuthenticationSHA256) Decode(src []byte) error {
	if len(src) < 4 {
		return errors.New("authentication message too short")
	}

	/*authType := binary.BigEndian.Uint32(src)

	if authType != AuthTypeSHA256 {
		return errors.New("bad auth type")
	}*/

	//authMechanisms := src[4:]

	readBuf := ReadBuf(src)
	dst.r = &readBuf

	/*authMechanisms := src[:74]
	for len(authMechanisms) > 1 {
		idx := bytes.IndexByte(authMechanisms, 0)
		if idx == -1 {
			//return &invalidMessageFormatErr{messageType: "AuthenticationSASL", details: "unterminated string"}
		}
		dst.AuthMechanisms = append(dst.AuthMechanisms, string(authMechanisms[:idx]))
		authMechanisms = authMechanisms[idx+1:]
	}*/

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *AuthenticationSHA256) Encode(dst []byte) ([]byte, error) {
	dst, sp := beginMessage(dst, 'R')
	dst = pgio.AppendUint32(dst, AuthTypeSASL)

	/*for _, s := range src.AuthMechanisms {
		dst = append(dst, []byte(s)...)
		dst = append(dst, 0)
	}
	dst = append(dst, 0)*/

	return finishMessage(dst, sp)
}

// MarshalJSON implements encoding/json.Marshaler.
func (src AuthenticationSHA256) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type           string
		AuthMechanisms []string
	}{
		Type: "AuthenticationSASL",
		//AuthMechanisms: src.AuthMechanisms,
	})
}

// todo ---------- GaussDB start ----------

func (dst *AuthenticationSHA256) GetR() *ReadBuf {
	return dst.r
}

// todo ---------- GaussDB end ----------
