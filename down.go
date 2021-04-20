package goose

import (
	"fmt"

	"gorm.io/gorm"
)

// Down rolls back a single migration from the current version.
func Down(db *gorm.DB, service, dir string) error {
	currentVersion, err := GetDBVersion(db, service)
	if err != nil {
		return err
	}

	migrations, err := CollectMigrations(service, dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	current, err := migrations.Current(currentVersion)
	if err != nil {
		return fmt.Errorf("no migration %v", currentVersion)
	}

	return current.Down(db)
}

// DownTo rolls back migrations to a specific version.
func DownTo(db *gorm.DB, service, dir string, version int64) error {
	migrations, err := CollectMigrations(service, dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	for {
		currentVersion, err := GetDBVersion(db, service)
		if err != nil {
			return err
		}

		current, err := migrations.Current(currentVersion)
		if err != nil {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if current.Version <= version {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if err = current.Down(db); err != nil {
			return err
		}
	}
}
