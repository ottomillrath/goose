package goose

import "gorm.io/gorm"

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(db *gorm.DB, service, dir string) error {
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
		return err
	}

	if err := current.Down(db); err != nil {
		return err
	}

	if err := current.Up(db); err != nil {
		return err
	}

	return nil
}
