package migration

import (
	"fmt"
	"sort"
	"utilities-golang/logs"
	"utilities-golang/persistent"

	"github.com/pkg/errors"
)

func NewSqlMigration(orm persistent.ORM, migrations map[int]*Script, logger logs.Logger) (Tool, error) {
	if orm == nil {
		return nil, errors.New("orm is required!")
	}

	if logger == nil {
		return nil, errors.New("logger is required!")
	}

	return &sql{orm: orm, migrations: migrations, logger: logger}, nil
}

func (s *sql) Up() error {
	if err := isMigrationTableExists(s); err != nil {
		return err
	}

	if isMigrationScriptsEmpty(s) {
		return nil
	}

	last, err := getLatestMigrationVersionFromDatabase(s)

	if err != nil {
		return err
	}

	if last == getLatestMigrationVersion(s) {
		s.logger.Infof("%s migration already up to date!", UpTag)
		return nil
	}

	keys := make([]int, 0)

	for k := range s.migrations {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	versions := make([]int, 0)

	// - first migration or migration table truncated
	if last == 0 {
		versions = keys
	} else {
		start := 0
		for i, key := range keys {
			if key == last {
				start = i + 1
				break
			}
		}
		versions = keys[start:]
	}

	for _, version := range versions {
		s.logger.Infof("%s begin up migration version %d", UpTag, version)
		script := s.migrations[version]

		// - check migration script
		if len(script.Up) == 0 {
			return errors.New(fmt.Sprintf("%s migration script %d can't be blank!", UpTag, version))
		}

		s.logger.Infof("%s executing migration version %d", UpTag, version)

		var outer error
		tx := s.orm.Begin()

		// - raw sql to exec multiple statements
		if _, err := tx.RawSql(script.Up); err != nil {
			outer = err
		}

		if err := tx.Exec("INSERT INTO "+TableName+" VALUES(?)", version); err != nil {
			outer = err
		}

		if outer != nil {
			s.logger.Errorf("%s failed to execute migration up script with version %d", UpTag, version)
			s.logger.Error(outer)
			s.logger.Infof("%s rollback migration with version %d", UpTag, version)

			if err := tx.Rollback(); err != nil {
				s.logger.Errorf("%s failed to rollback migration with version %d", UpTag, version)
				return errors.Wrapf(err, "failed to rollback migration with version %d", version)
			}

			s.logger.Infof("%s rollback migration version %d succeeded!", UpTag, version)

			return nil
		}

		// - commit changes
		if err := tx.Commit(); err != nil {
			return errors.Wrapf(err, "failed to commit transaction with version %d", version)
		}

		s.logger.Infof("%s migration with version %d migrated!", UpTag, version)
	}

	return nil
}

func (s *sql) Down() error {
	if err := isMigrationTableExists(s); err != nil {
		return err
	}

	if isMigrationScriptsEmpty(s) {
		return nil
	}

	version, err := getLatestMigrationVersionFromDatabase(s)

	if err != nil {
		return err
	}

	if version == 0 {
		s.logger.Infof("%s migrations table is empty, nothing to do", DownTag)
		return nil
	}

	keys := make([]int, 0)

	for k := range s.migrations {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	script := s.migrations[version]

	// - check migration script
	if len(script.Down) == 0 {
		return errors.New(fmt.Sprintf("%s script with version %d can't be empty", DownTag, version))
	}

	s.logger.Infof("%s begin down migration %d version", DownTag, version)
	tx := s.orm.Begin()

	var outer error

	// - execute migrations script
	if _, err := tx.RawSql(script.Down); err != nil {
		s.logger.Errorf("%s failed to execute migration down script with version %d", DownTag, version)
		outer = err
	}

	// - remove version greater than before from migrations table
	if err := tx.Exec("DELETE FROM migrations WHERE version >= ?", version); err != nil {
		s.logger.Errorf("%s failed to execute delete migration script %+v", DownTag, version)
		outer = err
	}

	if outer != nil {
		s.logger.Errorf("%s %s", DownTag, outer)
		s.logger.Infof("%s rollback migration version %d!", DownTag, version)

		if err := tx.Rollback(); err != nil {
			s.logger.Errorf("%s failed to rollback migration with version %d", DownTag, version)
			return errors.Wrapf(err, "failed to rollback migration with version %d", version)
		}

		s.logger.Infof("%s rollback succeeded!", DownTag)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "failed to commit down migration with version %d", version)
	}

	s.logger.Infof("%s migration version %d succeeded", DownTag, version)

	return nil
}

func (s *sql) Check() error {
	if err := isMigrationTableExists(s); err != nil {
		return err
	}

	if isMigrationScriptsEmpty(s) {
		return nil
	}

	version := getLatestMigrationVersion(s)

	if err := isAlreadyMigrated(s, version); err != nil {
		return err
	}

	return nil
}

func (s *sql) Truncate() error {
	if err := isMigrationTableExists(s); err != nil {
		return err
	}

	if err := s.orm.Exec("TRUNCATE TABLE " + TableName); err != nil {
		return errors.New(fmt.Sprintf("failed to truncate %s", TableName))
	}

	return nil
}

func (s *sql) Initialize() error {
	if err := isMigrationTableExists(s); err == nil {
		s.logger.Infof("%s table already exists, nothing to do!", TableName)
		return nil
	}

	tx := s.orm.Begin()
	query := `CREATE TABLE migrations(version bigint not null)`

	var outer error

	if err := tx.Exec(query); err != nil {
		s.logger.Errorf("failed to create %s table!", TableName)
		outer = err
	}

	if outer != nil {
		s.logger.Error(outer)
		s.logger.Info("begin rollback!")

		if err := tx.Rollback(); err != nil {
			return errors.Wrap(err, "failed to rollback transaction!")
		}

		s.logger.Info("rollback succeeded!")

		return nil
	}

	// - commit changes
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction!")
	}

	s.logger.Infof("%s table created!", TableName)

	return nil
}

// - private

func isMigrationScriptsEmpty(s *sql) bool {
	length := len(s.migrations)

	if length == 0 {
		s.logger.Info("migration script is empty, nothing to migrate!")
		return true
	}

	return false
}

func isMigrationTableExists(s *sql) error {
	query := fmt.Sprintf("SELECT 1 FROM %s", TableName)

	if _, err := s.orm.RawSql(query); err != nil {
		return errors.Wrapf(err, "migration table %s not found!", TableName)
	}

	return nil
}

func isAlreadyMigrated(s *sql, version int) error {
	rows, err := s.orm.RawSql("SELECT version FROM "+TableName+" ORDER BY ?", ColumnName)

	if err != nil {
		return errors.Wrapf(err, "failed to check migration with version %d", version)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			s.logger.Errorf("failed to close rows", err)
		}
	}()

	err = nil
	id, total := 0, 0
	found := false

	for rows.Next() {
		total += 1
		err = rows.Scan(&id)

		if err != nil {
			break
		}

		if id == version {
			found = true
			break
		}
	}

	// - migrations table is empty but migration file is not empty
	if total == 0 {
		return errors.New(fmt.Sprintf("migration %d is not migrated!", version))
	}

	// - failed row.Scan()
	if err != nil {
		return errors.Wrap(err, "failed to scan version column")
	}

	// - migrations table not empty but could not find migration with specific version
	if !found {
		return errors.New(fmt.Sprintf("migration %d is not migrated!", version))
	}

	return nil
}

func getLatestMigrationVersion(s *sql) int {
	keys := make([]int, 0)

	for k := range s.migrations {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	return keys[len(keys)-1]
}

func getLatestMigrationVersionFromDatabase(s *sql) (int, error) {
	query := fmt.Sprintf("SELECT %s FROM migrations ORDER BY %s DESC", ColumnName, ColumnName)

	rows, err := s.orm.RawSql(query)

	if err != nil {
		return 0, errors.Wrap(err, "failed to get latest migration version from database!")
	}

	defer func() {
		if err := rows.Close(); err != nil {
			s.logger.Errorf("failed to close rows ", err)
		}
	}()

	version, err := 0, nil

	for rows.Next() {
		err = rows.Scan(&version)

		if err != nil {
			break
		}
		break
	}

	if err != nil {
		return 0, errors.Wrap(err, "failed to scan version column!")
	}

	return version, nil
}
