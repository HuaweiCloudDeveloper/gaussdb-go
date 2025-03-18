package gaussdbpool

import (
	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgconn"
)

type errBatchResults struct {
	err error
}

func (br errBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, br.err
}

func (br errBatchResults) Query() (gaussdb.Rows, error) {
	return errRows{err: br.err}, br.err
}

func (br errBatchResults) QueryRow() gaussdb.Row {
	return errRow{err: br.err}
}

func (br errBatchResults) Close() error {
	return br.err
}

type poolBatchResults struct {
	br gaussdb.BatchResults
	c  *Conn
}

func (br *poolBatchResults) Exec() (pgconn.CommandTag, error) {
	return br.br.Exec()
}

func (br *poolBatchResults) Query() (gaussdb.Rows, error) {
	return br.br.Query()
}

func (br *poolBatchResults) QueryRow() gaussdb.Row {
	return br.br.QueryRow()
}

func (br *poolBatchResults) Close() error {
	err := br.br.Close()
	if br.c != nil {
		br.c.Release()
		br.c = nil
	}
	return err
}
