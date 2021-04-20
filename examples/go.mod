module github.com/ottomillrath/goose/examples

go 1.16

replace github.com/ottomillrath/goose => ../

require (
	github.com/mattn/go-sqlite3 v1.14.7 // indirect
	github.com/ottomillrath/goose v0.0.0-00010101000000-000000000000
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.21.8
)
