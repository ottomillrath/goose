package goose

import (
	"regexp"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// Run a migration specified in raw SQL.
//
// Sections of the script can be annotated with a special comment,
// starting with "-- +goose" to specify whether the section should
// be applied during an Up or Down migration
//
// All statements following an Up or Down directive are grouped together
// until another direction directive is found.
func runSQLMigration(db *gorm.DB, statements []string, useTx bool, service string, v int64, direction bool) error {
	if useTx {
		// TRANSACTION.

		verboseInfo("Begin transaction")

		tx := db.Begin()
		if tx.Error != nil {
			return errors.Wrap(tx.Error, "failed to begin transaction")
		}

		for _, query := range statements {
			verboseInfo("Executing statement: %s\n", clearStatement(query))
			if r := tx.Exec(query); r.Error != nil {
				verboseInfo("Rollback transaction")
				tx.Rollback()
				return errors.Wrapf(r.Error, "failed to execute SQL query %q", clearStatement(query))
			}
		}

		if direction {
			if r := tx.Exec(GetDialect().insertVersionSQL(service), v, direction); r.Error != nil {
				verboseInfo("Rollback transaction")
				tx.Rollback()
				return errors.Wrap(r.Error, "failed to insert new goose version")
			}
		} else {
			if r := tx.Exec(GetDialect().deleteVersionSQL(service), v); r.Error != nil {
				verboseInfo("Rollback transaction")
				tx.Rollback()
				return errors.Wrap(r.Error, "failed to delete goose version")
			}
		}

		verboseInfo("Commit transaction")
		if r := tx.Commit(); r.Error != nil {
			return errors.Wrap(r.Error, "failed to commit transaction")
		}

		return nil
	}

	// NO TRANSACTION.
	for _, query := range statements {
		verboseInfo("Executing statement: %s", clearStatement(query))
		if r := db.Exec(query); r.Error != nil {
			return errors.Wrapf(r.Error, "failed to execute SQL query %q", clearStatement(query))
		}
	}
	if r := db.Exec(GetDialect().insertVersionSQL(service), v, direction); r.Error != nil {
		return errors.Wrap(r.Error, "failed to insert new goose version")
	}

	return nil
}

const (
	grayColor  = "\033[90m"
	resetColor = "\033[00m"
)

func verboseInfo(s string, args ...interface{}) {
	if verbose {
		log.Printf(grayColor+s+resetColor, args...)
	}
}

var (
	matchSQLComments = regexp.MustCompile(`(?m)^--.*$[\r\n]*`)
	matchEmptyEOL    = regexp.MustCompile(`(?m)^$[\r\n]*`) // TODO: Duplicate
)

func clearStatement(s string) string {
	s = matchSQLComments.ReplaceAllString(s, ``)
	return matchEmptyEOL.ReplaceAllString(s, ``)
}
