package main

// This is custom goose binary with sqlite3 support only.

import (
	"flag"
	"log"
	"os"

	"github.com/ottomillrath/goose/v2"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	flags   = flag.NewFlagSet("goose", flag.ExitOnError)
	dir     = flags.String("dir", ".", "directory with migration files")
	service = flags.String("service", "default", "")
)

func main() {
	flags.Parse(os.Args[1:])
	args := flags.Args()

	if len(args) < 2 {
		flags.Usage()
		return
	}

	dbstring, command := args[1], args[3]

	err := goose.SetDialect("sqlite3")
	if err != nil {
		log.Fatalf("goose: failed to set dialect: %v\n", err)
	}
	db, err := gorm.Open(sqlite.Open(dbstring), &gorm.Config{})
	if err != nil {
		log.Fatalf("goose: failed to open DB: %v\n", err)
	}

	defer func() {
		internalDb, err := db.DB()
		if err != nil {
			log.Fatalf("goose: failed to get internal DB: %v\n", err)
		}
		if err := internalDb.Close(); err != nil {
			log.Fatalf("goose: failed to close DB: %v\n", err)
		}
	}()

	arguments := []string{}
	if len(args) > 3 {
		arguments = append(arguments, args[3:]...)
	}

	if err := goose.Run(command, db, *service, *dir, arguments...); err != nil {
		log.Fatalf("goose %v: %v", command, err)
	}
}
