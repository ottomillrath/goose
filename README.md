# goose

Goose is a database migration tool. Manage your database schema by creating incremental SQL changes or Go functions.

[![GoDoc Widget]][GoDoc] [![Travis Widget]][Travis]

### Goals of this fork

`github.com/ottomillrath/goose` is a fork of `github.com/pressly/goose` with the following changes:
* Add `service` parameter to uniquely identify and separate various migration contexts
* Change the internal connection to an instance of `*gorm.DB`, from [GORM](https://github.com/go-gorm/gorm)
* Use go modules

_The CLI tool is not compatible yet (WIP)._

_The rest of this README is the same as the one in the parent repository._

# Install

    $ go get -u github.com/ottomillrath/goose/cmd/goose

This will install the `goose` binary to your `$GOPATH/bin` directory.

For a lite version of the binary without DB connection dependent commands, use the exclusive build tags:

    $ go build -tags='no_postgres no_mysql no_sqlite3' -i -o goose ./cmd/goose


# Usage

```
Usage: goose [OPTIONS] DRIVER DBSTRING COMMAND

Drivers:
    postgres
    mysql
    sqlite3
    mssql
    redshift

Examples:
    goose sqlite3 ./foo.db status
    goose sqlite3 ./foo.db create init sql
    goose sqlite3 ./foo.db create add_some_column sql
    goose sqlite3 ./foo.db create fetch_user_data go
    goose sqlite3 ./foo.db up

    goose postgres "user=postgres dbname=postgres sslmode=disable" status
    goose mysql "user:password@/dbname?parseTime=true" status
    goose redshift "postgres://user:password@qwerty.us-east-1.redshift.amazonaws.com:5439/db" status
    goose tidb "user:password@/dbname?parseTime=true" status
    goose mssql "sqlserver://user:password@dbname:1433?database=master" status

Options:

  -dir string
    	directory with migration files (default ".")
  -table string
    	migrations table name (default "goose_db_version")
  -h	print help
  -v	enable verbose mode
  -version
    	print version

Commands:
    up                   Migrate the DB to the most recent version available
    up-by-one            Migrate the DB up by 1
    up-to VERSION        Migrate the DB to a specific VERSION
    down                 Roll back the version by 1
    down-to VERSION      Roll back to a specific VERSION
    redo                 Re-run the latest migration
    reset                Roll back all migrations
    status               Dump the migration status for the current DB
    version              Print the current version of the database
    create NAME [sql|go] Creates new migration file with the current timestamp
    fix                  Apply sequential ordering to migrations
```

## create

Create a new SQL migration.

    $ goose create add_some_column sql
    $ Created new file: 20170506082420_add_some_column.sql

Edit the newly created file to define the behavior of your migration.

You can also create a Go migration, if you then invoke it with [your own goose binary](#go-migrations):

    $ goose create fetch_user_data go
    $ Created new file: 20170506082421_fetch_user_data.go

## up

Apply all available migrations.

    $ goose up
    $ OK    001_basics.sql
    $ OK    002_next.sql
    $ OK    003_and_again.go

## up-to

Migrate up to a specific version.

    $ goose up-to 20170506082420
    $ OK    20170506082420_create_table.sql

## up-by-one

Migrate up a single migration from the current version

    $ goose up-by-one
    $ OK    20170614145246_change_type.sql

## down

Roll back a single migration from the current version.

    $ goose down
    $ OK    003_and_again.go

## down-to

Roll back migrations to a specific version.

    $ goose down-to 20170506082527
    $ OK    20170506082527_alter_column.sql

## redo

Roll back the most recently applied migration, then run it again.

    $ goose redo
    $ OK    003_and_again.go
    $ OK    003_and_again.go

## status

Print the status of all migrations:

    $ goose status
    $   Applied At                  Migration
    $   =======================================
    $   Sun Jan  6 11:25:03 2013 -- 001_basics.sql
    $   Sun Jan  6 11:25:03 2013 -- 002_next.sql
    $   Pending                  -- 003_and_again.go

Note: for MySQL [parseTime flag](https://github.com/go-sql-driver/mysql#parsetime) must be enabled.

## version

Print the current version of the database:

    $ goose version
    $ goose: version 002

# Migrations

goose supports migrations written in SQL or in Go.

## SQL Migrations

A sample SQL migration looks like:

```sql
-- +goose Up
CREATE TABLE post (
    id int NOT NULL,
    title text,
    body text,
    PRIMARY KEY(id)
);

-- +goose Down
DROP TABLE post;
```

Notice the annotations in the comments. Any statements following `-- +goose Up` will be executed as part of a forward migration, and any statements following `-- +goose Down` will be executed as part of a rollback.

By default, all migrations are run within a transaction. Some statements like `CREATE DATABASE`, however, cannot be run within a transaction. You may optionally add `-- +goose NO TRANSACTION` to the top of your migration
file in order to skip transactions within that specific migration file. Both Up and Down migrations within this file will be run without transactions.

By default, SQL statements are delimited by semicolons - in fact, query statements must end with a semicolon to be properly recognized by goose.

More complex statements (PL/pgSQL) that have semicolons within them must be annotated with `-- +goose StatementBegin` and `-- +goose StatementEnd` to be properly recognized. For example:

```sql
-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION histories_partition_creation( DATE, DATE )
returns void AS $$
DECLARE
  create_query text;
BEGIN
  FOR create_query IN SELECT
      'CREATE TABLE IF NOT EXISTS histories_'
      || TO_CHAR( d, 'YYYY_MM' )
      || ' ( CHECK( created_at >= timestamp '''
      || TO_CHAR( d, 'YYYY-MM-DD 00:00:00' )
      || ''' AND created_at < timestamp '''
      || TO_CHAR( d + INTERVAL '1 month', 'YYYY-MM-DD 00:00:00' )
      || ''' ) ) inherits ( histories );'
    FROM generate_series( $1, $2, '1 month' ) AS d
  LOOP
    EXECUTE create_query;
  END LOOP;  -- LOOP END
END;         -- FUNCTION END
$$
language plpgsql;
-- +goose StatementEnd
```

## Go Migrations

1. Create your own goose binary, see [example](./examples/go-migrations)
2. Import `github.com/ottomillrath/goose`
3. Register your migration functions
4. Run goose command, ie. `goose.Up(db *gorm.DB, dir string)`

A [sample Go migration 00002_users_add_email.go file](./examples/go-migrations/00002_rename_root.go) looks like:

```go
package migrations

import (
	"database/sql"

	"github.com/ottomillrath/goose"
)

func init() {
	goose.AddMigration(Up, Down)
}

func Up(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE users SET username='admin' WHERE username='root';")
	if err != nil {
		return err
	}
	return nil
}

func Down(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE users SET username='root' WHERE username='admin';")
	if err != nil {
		return err
	}
	return nil
}
```

# Hybrid Versioning
Please, read the [versioning problem](https://github.com/ottomillrath/goose/issues/63#issuecomment-428681694) first.

We strongly recommend adopting a hybrid versioning approach, using both timestamps and sequential numbers. Migrations created during the development process are timestamped and sequential versions are ran on production. We believe this method will prevent the problem of conflicting versions when writing software in a team environment.

To help you adopt this approach, `create` will use the current timestamp as the migration version. When you're ready to deploy your migrations in a production environment, we also provide a helpful `fix` command to convert your migrations into sequential order, while preserving the timestamp ordering. We recommend running `fix` in the CI pipeline, and only when the migrations are ready for production.

## License

Licensed under [MIT License](./LICENSE)

[GoDoc]: https://godoc.org/github.com/ottomillrath/goose
[GoDoc Widget]: https://godoc.org/github.com/ottomillrath/goose?status.svg
[Travis]: https://travis-ci.org/ottomillrath/goose
[Travis Widget]: https://travis-ci.org/ottomillrath/goose.svg?branch=master
