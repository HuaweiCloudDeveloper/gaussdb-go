# gaussdb-go Examples
This directory contains practical examples of frequently used operations for quick reference and learning.

## Prerequisites

Before running the examples, make sure you have:  

1. **GaussDB or OpenGauss database running**
   ```bash
   # Using Docker (recommended)
   docker run --name gaussdb-test \
     -e GS_PASSWORD=Gaussdb@123 \
     -p 5433:5432 \
     -d opengauss/opengauss:7.0.0-RC1.B023
   ```

2. **Golang environment**
- install Golang: https://go.dev/doc/install
- install the dependencies: `go mod tidy`

## Examples Overview

| Example                                  | Description                    | Features                     |
|------------------------------------------|--------------------------------|------------------------------|
| [authentication](authentication/main.go) | GaussDB authentication methods | SHA256, MD5_SHA256 auth      |
| [basic](basic/main.go)                   | Basic operations               | Create, insert, queries      |
| [copy](copy/main.go)                     | COPY operations                | Bulk data import/export      |
| [pool](pool/main.go)                     | Pool connections               | Use connection pool          |
| [ssl](ssl/main.go)                       | SSL options                    | Connect with ssl option      |
| [transactions](transactions/main.go)     | Transaction management         | Commit, rollback, savepoints |
| [types](types/main.go)                   | Data type conversions          | Most supported GaussDB types |

## Running Example
```bash
# Run a specific example
go run examples/operations/basic/main.go
```

## Configuration

### Environment Variables

Set these environment variables to customize database connection:

```bash
export DATABASE_URL="host=localhost user=gaussdb password=Gaussdb@123 database=postgres port=5433"
export GAUSSDB_HOST="localhost"
export GAUSSDB_PORT="5433"
export GAUSSDB_USER="gaussdb"
export GAUSSDB_PASSWORD="Gaussdb@123"
export GAUSSDB_DATABASE="postgres"
```

### Connection String Format

```
host=localhost user=gaussdb password=Gaussdb@123 database=postgres port=5433 sslmode=disable
```

## Common Issues and Solutions

### 1. Connection Refused

**Problem**: `Connection refused (os error 10061)`

**Solution**:
- Ensure GaussDB/OpenGauss is running
- Check to ensure the specified port used by the database
- Verify firewall settings

### 2. Authentication Failed

**Problem**: `password authentication failed`

**Solution**:
- Check username and password
- Verify authentication method in pg_hba.conf
- Ensure user has proper permissions

### 3. Database Does Not Exist

**Problem**: `database "test" does not exist`

**Solution**:
- Use existing database (usually "postgres")
- Create database first: `CREATE DATABASE test;`

### 4. SSL/TLS Issues

**Problem**: SSL connection errors

**Solution**:
- Use `sslmode=disable` for testing
- Install proper certificates for production

## Performance Tips

1. **Use Connection Pooling**: For high-concurrency applications
2. **Prepared Statements**: For repeated queries
3. **Batch Operations**: Use COPY for bulk data

## Security Best Practices

1. **Use TLS**: Always enable TLS in production
2. **Strong Passwords**: Use complex passwords
3. **Least Privilege**: Grant minimal required permissions
4. **Parameter Binding**: Always use parameterized queries
5. **Connection Limits**: Set appropriate connection limits

## Contributing

To add new examples:

1. Create a new directory in the `examples` directory
2. Add `main.go` in the directory
3. Include comprehensive error handling
4. Add documentation comments
5. Update this README.md

## Support

- [GitHub Issues](https://github.com/HuaweiCloudDeveloper/gaussdb-go/issues)
- [GaussDB Documentation](https://docs.opengauss.org/)