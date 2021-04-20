module github.com/ottomillrath/goose/cmd/goose

go 1.16

replace github.com/ottomillrath/goose => ../../

require (
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/lib/pq v1.10.0
	github.com/mattn/go-sqlite3 v1.14.7
	github.com/ottomillrath/goose v0.0.0-00010101000000-000000000000
)
