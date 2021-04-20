package goose

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// MigrationRecord struct.
type MigrationRecord struct {
	VersionID int64
	TStamp    time.Time
	IsApplied bool // was this a result of up() or down()
}

// MigrationFn used in go migrations.
type MigrationFn func(tx *gorm.DB) error

// Migration struct.
type Migration struct {
	Service    string
	Version    int64
	Next       int64  // next version, or -1 if none
	Previous   int64  // previous version, -1 if none
	Source     string // path to .sql script or go file
	Registered bool
	UpFn       MigrationFn // Up go migration function
	DownFn     MigrationFn // Down go migration function
}

func (m *Migration) String() string {
	return fmt.Sprintf(m.Source)
}

// Up runs an up migration.
func (m *Migration) Up(db *gorm.DB) error {
	if err := m.run(db, true); err != nil {
		return err
	}
	return nil
}

// Down runs a down migration.
func (m *Migration) Down(db *gorm.DB) error {
	if err := m.run(db, false); err != nil {
		return err
	}
	return nil
}

func (m *Migration) run(db *gorm.DB, direction bool) error {
	switch filepath.Ext(m.Source) {
	case ".sql":
		f, err := os.Open(m.Source)
		if err != nil {
			return errors.Wrapf(err, "ERROR %v: failed to open SQL migration file", filepath.Base(m.Source))
		}
		defer f.Close()

		statements, useTx, err := parseSQLMigration(f, direction)
		if err != nil {
			return errors.Wrapf(err, "ERROR %v: failed to parse SQL migration file", filepath.Base(m.Source))
		}

		if err := runSQLMigration(db, statements, useTx, m.Service, m.Version, direction); err != nil {
			return errors.Wrapf(err, "ERROR %v: failed to run SQL migration", filepath.Base(m.Source))
		}

		if len(statements) > 0 {
			log.Println("OK   ", filepath.Base(m.Source))
		} else {
			log.Println("EMPTY", filepath.Base(m.Source))
		}

	case ".go":
		if !m.Registered {
			return errors.Errorf("ERROR %v: failed to run Go migration: Go functions must be registered and built into a custom binary (see https://github.com/ottomillrath/goose/tree/master/examples/go-migrations)", m.Source)
		}
		tx := db.Begin()
		if tx.Error != nil {
			return errors.Wrap(tx.Error, "ERROR failed to begin transaction")
		}

		fn := m.UpFn
		if !direction {
			fn = m.DownFn
		}

		if fn != nil {
			// Run Go migration function.
			if err := fn(tx); err != nil {
				tx.Rollback()
				return errors.Wrapf(err, "ERROR %v: failed to run Go migration function %T", filepath.Base(m.Source), fn)
			}
		}

		if direction {
			if r := tx.Exec(GetDialect().insertVersionSQL(m.Service), m.Version, direction); r.Error != nil {
				tx.Rollback()
				return errors.Wrap(r.Error, "ERROR failed to execute transaction")
			}
		} else {
			if r := tx.Exec(GetDialect().deleteVersionSQL(m.Service), m.Version); r.Error != nil {
				tx.Rollback()
				return errors.Wrap(r.Error, "ERROR failed to execute transaction")
			}
		}

		if r := tx.Commit(); r.Error != nil {
			return errors.Wrap(r.Error, "ERROR failed to commit transaction")
		}

		if fn != nil {
			log.Println("OK   ", filepath.Base(m.Source))
		} else {
			log.Println("EMPTY", filepath.Base(m.Source))
		}

		return nil
	}

	return nil
}

// NumericComponent looks for migration scripts with names in the form:
// XXX_descriptivename.ext where XXX specifies the version number
// and ext specifies the type of migration
func NumericComponent(name string) (int64, error) {
	base := filepath.Base(name)

	if ext := filepath.Ext(base); ext != ".go" && ext != ".sql" {
		return 0, errors.New("not a recognized migration file type")
	}

	idx := strings.Index(base, "_")
	if idx < 0 {
		return 0, errors.New("no separator found")
	}

	n, e := strconv.ParseInt(base[:idx], 10, 64)
	if e == nil && n <= 0 {
		return 0, errors.New("migration IDs must be greater than zero")
	}

	return n, e
}
