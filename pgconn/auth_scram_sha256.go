// SCRAM-SHA-256 authentication
//
// Resources:
//   https://tools.ietf.org/html/rfc5802
//   https://tools.ietf.org/html/rfc8265
//   https://www.postgresql.org/docs/current/sasl-authentication.html
//
// Inspiration drawn from other implementations:
//   https://github.com/lib/pq/pull/608
//   https://github.com/lib/pq/pull/788
//   https://github.com/lib/pq/pull/833

package pgconn

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgproto3"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/text/secure/precis"
)

const clientSha256NonceLen = 18

func (cn *PgConn) authSha256(r *readBuf) error {
	/*if r.int32() != pgproto3.AuthTypeSHA256 {
		return errors.New("bad auth type")
	}*/

	if cn.frontend.GetAuthType() != pgproto3.AuthTypeSHA256 {
		return errors.New("bad auth type")
	}

	// 这里在openGauss为sha256加密办法，主要代码流程来自jdbc相关实现
	//passwordStoredMethod := r.int32()
	passwordStoredMethod := cn.frontend.GetPasswordStoredMethod()
	digest := ""
	if len(cn.config.Password) == 0 {
		//errorf("The server requested password-based authentication, but no password was provided.")
		return fmt.Errorf("The server requested password-based authentication, but no password was provided.")
	}

	if passwordStoredMethod == PlainPassword || passwordStoredMethod == Sha256Password {
		random64code := string(r.next(64))
		token := string(r.next(8))
		serverIteration := r.int32()
		result := RFC5802Algorithm(cn.config.Password, random64code, token, "", serverIteration, "sha256")
		if len(result) == 0 {
			//errorf("Invalid username/password,login denied.")
			return fmt.Errorf("Invalid username/password,login denied.")
		}

		w := cn.writeBuf('p')
		w.buf = []byte("p")
		w.pos = 1
		w.int32(4 + len(result) + 1)
		w.bytes(result)
		w.byte(0)

		cn.send(w)

		t, r := cn.recv()

		if t != 'R' {
			//errorf("unexpected password response: %q", t)
			return fmt.Errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			//errorf("unexpected authentication response: %q", t)
			return fmt.Errorf("unexpected authentication response: %q", t)
		}
		// return
	} else if passwordStoredMethod == Md5Password {
		s := string(r.next(4))
		digest = "md5" + md5s(md5s(cn.config.Password+cn.config.User)+s)
		w := cn.writeBuf('p')
		w.int16(4 + len(digest) + 1)
		w.string(digest)
		w.byte(0)

		cn.send(w)

		t, r := cn.recv()

		if t != 'R' {
			//errorf("unexpected password response: %q", t)
			return fmt.Errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			//errorf("unexpected authentication response: %q", t)
			return fmt.Errorf("unexpected authentication response: %q", t)
		}
	} else {
		//errorf("The  password-stored method is not supported ,must be plain, md5 or sha256.")
		return fmt.Errorf("The  password-stored method is not supported ,must be plain, md5 or sha256.")
	}

	return nil
}

// Perform SCRAM authentication.
func (c *PgConn) scramSha256Auth(serverAuthMechanisms []string, r *pgproto3.ReadBuf) error {
	err := c.authSha256((*readBuf)(r))
	if err != nil {
		return err
	}

	/*sc, err := newScramSha256Client(serverAuthMechanisms, c.config.Password)
	if err != nil {
		return err
	}

	// Send client-first-message in a SASLInitialResponse
	saslInitialResponse := &pgproto3.SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256",
		Data:          sc.clientSha256FirstMessage(),
	}
	c.frontend.Send(saslInitialResponse)
	err = c.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		return err
	}

	// Receive server-first-message payload in an AuthenticationSASLContinue.
	saslContinue, err := c.rxSASLSha256Continue()
	if err != nil {
		return err
	}
	err = sc.recvServerSha256FirstMessage(saslContinue.Data)
	if err != nil {
		return err
	}

	// Send client-final-message in a SASLResponse
	saslResponse := &pgproto3.SASLResponse{
		Data: []byte(sc.clientSha256FinalMessage()),
	}
	c.frontend.Send(saslResponse)
	err = c.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		return err
	}

	// Receive server-final-message payload in an AuthenticationSASLFinal.
	saslFinal, err := c.rxSASLSha256Final()
	if err != nil {
		return err
	}
	return sc.recvServerSha256FinalMessage(saslFinal.Data)*/

	return nil
}

func (c *PgConn) rxSASLSha256Continue() (*pgproto3.AuthenticationSASLContinue, error) {
	msg, err := c.receiveMessage()
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case *pgproto3.AuthenticationSASLContinue:
		return m, nil
	case *pgproto3.ErrorResponse:
		return nil, ErrorResponseToPgError(m)
	}

	return nil, fmt.Errorf("expected AuthenticationSASLContinue message but received unexpected message %T", msg)
}

func (c *PgConn) rxSASLSha256Final() (*pgproto3.AuthenticationSASLFinal, error) {
	msg, err := c.receiveMessage()
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case *pgproto3.AuthenticationSASLFinal:
		return m, nil
	case *pgproto3.ErrorResponse:
		return nil, ErrorResponseToPgError(m)
	}

	return nil, fmt.Errorf("expected AuthenticationSASLFinal message but received unexpected message %T", msg)
}

type scramSha256Client struct {
	serverAuthMechanisms []string
	password             []byte
	clientNonce          []byte

	clientFirstMessageBare []byte

	serverFirstMessage   []byte
	clientAndServerNonce []byte
	salt                 []byte
	iterations           int

	saltedPassword []byte
	authMessage    []byte
}

func newScramSha256Client(serverAuthMechanisms []string, password string) (*scramSha256Client, error) {
	sc := &scramSha256Client{
		serverAuthMechanisms: serverAuthMechanisms,
	}

	// Ensure server supports SCRAM-SHA-256
	hasScramSHA256 := false
	for _, mech := range sc.serverAuthMechanisms {
		if mech == "SCRAM-SHA-256" {
			hasScramSHA256 = true
			break
		}
	}
	if !hasScramSHA256 {
		return nil, errors.New("server does not support SCRAM-SHA-256")
	}

	// precis.OpaqueString is equivalent to SASLprep for password.
	var err error
	sc.password, err = precis.OpaqueString.Bytes([]byte(password))
	if err != nil {
		// PostgreSQL allows passwords invalid according to SCRAM / SASLprep.
		sc.password = []byte(password)
	}

	buf := make([]byte, clientSha256NonceLen)
	_, err = rand.Read(buf)
	if err != nil {
		return nil, err
	}
	sc.clientNonce = make([]byte, base64.RawStdEncoding.EncodedLen(len(buf)))
	base64.RawStdEncoding.Encode(sc.clientNonce, buf)

	return sc, nil
}

func (sc *scramSha256Client) clientSha256FirstMessage() []byte {
	sc.clientFirstMessageBare = []byte(fmt.Sprintf("n=,r=%s", sc.clientNonce))
	return []byte(fmt.Sprintf("n,,%s", sc.clientFirstMessageBare))
}

func (sc *scramSha256Client) recvServerSha256FirstMessage(serverFirstMessage []byte) error {
	sc.serverFirstMessage = serverFirstMessage
	buf := serverFirstMessage
	if !bytes.HasPrefix(buf, []byte("r=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include r=")
	}
	buf = buf[2:]

	idx := bytes.IndexByte(buf, ',')
	if idx == -1 {
		return errors.New("invalid SCRAM server-first-message received from server: did not include s=")
	}
	sc.clientAndServerNonce = buf[:idx]
	buf = buf[idx+1:]

	if !bytes.HasPrefix(buf, []byte("s=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include s=")
	}
	buf = buf[2:]

	idx = bytes.IndexByte(buf, ',')
	if idx == -1 {
		return errors.New("invalid SCRAM server-first-message received from server: did not include i=")
	}
	saltStr := buf[:idx]
	buf = buf[idx+1:]

	if !bytes.HasPrefix(buf, []byte("i=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include i=")
	}
	buf = buf[2:]
	iterationsStr := buf

	var err error
	sc.salt, err = base64.StdEncoding.DecodeString(string(saltStr))
	if err != nil {
		return fmt.Errorf("invalid SCRAM salt received from server: %w", err)
	}

	sc.iterations, err = strconv.Atoi(string(iterationsStr))
	if err != nil || sc.iterations <= 0 {
		return fmt.Errorf("invalid SCRAM iteration count received from server: %w", err)
	}

	if !bytes.HasPrefix(sc.clientAndServerNonce, sc.clientNonce) {
		return errors.New("invalid SCRAM nonce: did not start with client nonce")
	}

	if len(sc.clientAndServerNonce) <= len(sc.clientNonce) {
		return errors.New("invalid SCRAM nonce: did not include server nonce")
	}

	return nil
}

func (sc *scramSha256Client) clientSha256FinalMessage() string {
	clientFinalMessageWithoutProof := []byte(fmt.Sprintf("c=biws,r=%s", sc.clientAndServerNonce))

	sc.saltedPassword = pbkdf2.Key([]byte(sc.password), sc.salt, sc.iterations, 32, sha256.New)
	sc.authMessage = bytes.Join([][]byte{sc.clientFirstMessageBare, sc.serverFirstMessage, clientFinalMessageWithoutProof}, []byte(","))

	clientProof := computeClientSha256Proof(sc.saltedPassword, sc.authMessage)

	return fmt.Sprintf("%s,p=%s", clientFinalMessageWithoutProof, clientProof)
}

func (sc *scramSha256Client) recvServerSha256FinalMessage(serverFinalMessage []byte) error {
	if !bytes.HasPrefix(serverFinalMessage, []byte("v=")) {
		return errors.New("invalid SCRAM server-final-message received from server")
	}

	serverSignature := serverFinalMessage[2:]

	if !hmac.Equal(serverSignature, computeServerSha256Signature(sc.saltedPassword, sc.authMessage)) {
		return errors.New("invalid SCRAM ServerSignature received from server")
	}

	return nil
}

func computeSha256HMAC(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func computeClientSha256Proof(saltedPassword, authMessage []byte) []byte {
	clientKey := computeSha256HMAC(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := computeSha256HMAC(storedKey[:], authMessage)

	clientProof := make([]byte, len(clientSignature))
	for i := 0; i < len(clientSignature); i++ {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	buf := make([]byte, base64.StdEncoding.EncodedLen(len(clientProof)))
	base64.StdEncoding.Encode(buf, clientProof)
	return buf
}

func computeServerSha256Signature(saltedPassword []byte, authMessage []byte) []byte {
	serverKey := computeSha256HMAC(saltedPassword, []byte("Server Key"))
	serverSignature := computeSha256HMAC(serverKey, authMessage)
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(serverSignature)))
	base64.StdEncoding.Encode(buf, serverSignature)
	return buf
}
