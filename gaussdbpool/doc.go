// package gaussdbpool is a concurrency-safe connection pool for gaussdb.
/*
gaussdbpool implements a nearly identical interface to pgx connections.

Creating a Pool

The primary way of creating a pool is with [gaussdbpool.New]:

    pool, err := gaussdbpool.New(context.Background(), os.Getenv("DATABASE_URL"))

The database connection string can be in URL or keyword/value format. PostgreSQL settings, pgx settings, and pool settings can be
specified here. In addition, a config struct can be created by [ParseConfig].

    config, err := gaussdbpool.ParseConfig(os.Getenv("DATABASE_URL"))
    if err != nil {
        // ...
    }
    config.AfterConnect = func(ctx context.Context, conn *gaussdb.Conn) error {
        // do something with every new connection
    }

    pool, err := gaussdbpool.NewWithConfig(context.Background(), config)

A pool returns without waiting for any connections to be established. Acquire a connection immediately after creating
the pool to check if a connection can successfully be established.
*/
package gaussdbpool
