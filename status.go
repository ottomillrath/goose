package goose

import (
	"database/sql"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// Status prints the status of all migrations.
func Status(db *gorm.DB, service, dir string) error {
	// collect all migrations
	migrations, err := CollectMigrations(service, dir, minVersion, maxVersion)
	if err != nil {
		return errors.Wrap(err, "failed to collect migrations")
	}

	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db, service); err != nil {
		return errors.Wrap(err, "failed to ensure DB version")
	}

	log.Println("    Applied At                  Migration")
	log.Println("    =======================================")
	for _, migration := range migrations {
		if err := printMigrationStatus(db, migration.Version, filepath.Base(migration.Source), service); err != nil {
			return errors.Wrap(err, "failed to print status")
		}
	}

	return nil
}

func printMigrationStatus(db *gorm.DB, version int64, script string, service string) error {
	q := GetDialect().migrationSQL(service)

	var row MigrationRecord

	internalDb, err := db.DB()
	if err != nil {
		return err
	}

	err = internalDb.QueryRow(q, version).Scan(&row.TStamp, &row.IsApplied)
	if err != nil && err != sql.ErrNoRows {
		return errors.Wrap(err, "failed to query the latest migration")
	}

	var appliedAt string
	if row.IsApplied {
		appliedAt = row.TStamp.Format(time.ANSIC)
	} else {
		appliedAt = "Pending"
	}

	log.Printf("    %-24s -- %v\n", appliedAt, script)
	return nil
}
