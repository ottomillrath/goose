package main

import (
	"github.com/ottomillrath/goose/v2"
	"gorm.io/gorm"
)

func init() {
	goose.AddMigration("default", Up00002, Down00002)
}

func Up00002(tx *gorm.DB) error {
	r := tx.Exec("UPDATE users SET username='admin' WHERE username='root';")
	if r.Error != nil {
		return r.Error
	}
	return nil
}

func Down00002(tx *gorm.DB) error {
	r := tx.Exec("UPDATE users SET username='root' WHERE username='admin';")
	if r.Error != nil {
		return r.Error
	}
	return nil
}
