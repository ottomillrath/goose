package goose

import "gorm.io/gorm"

// Version prints the current version of the database.
func Version(db *gorm.DB, service, dir string) error {
	current, err := GetDBVersion(db, service)
	if err != nil {
		return err
	}

	log.Printf("goose: service %s version %v\n", service, current)
	return nil
}

var tableName = "goose_db_version"

// TableName returns goose db version table name
func TableName() string {
	return tableName
}

// SetTableName set goose db version table name
func SetTableName(n string) {
	tableName = n
}
