package migration

import (
	"context"
	"fmt"
	"sort"

	"github.com/jajotz/utilities-golang/logs"
	"github.com/jajotz/utilities-golang/persistent/mongo"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewNoSqlMigration(mongo mongo.Mongo, migrations map[int]*NoSqlScript, logger logs.Logger) (Tool, error) {
	if mongo == nil {
		return nil, errors.New("mongo is required!")
	}

	if logger == nil {
		return nil, errors.New("logger is required!")
	}

	return &nosql{orm: mongo, migrations: migrations, logger: logger}, nil
}

func (n *nosql) Up() error {
	if isNoSqlMigrationsEmpty(n) {
		return nil
	}

	migrated := make([]nosqlcollection, 0)

	// - get all migration from database
	callback := func(cursor mongo.Cursor, err error) error {
		if err != nil {
			return errors.Wrap(err, "")
		}

		for cursor.Next(context.Background()) {
			coll := nosqlcollection{}
			if err := cursor.Decode(&coll); err != nil {
				return errors.Wrap(err, "")
			}
			migrated = append(migrated, coll)
		}
		return nil
	}

	if err := n.orm.Find(TableName, bson.D{}, callback, options.Find().SetSort(bson.D{{"version", -1}})); err != nil {
		return err
	}

	last := 0
	if len(migrated) != 0 {
		last = migrated[0].Version
	}

	if last >= getLatestNoSqlMigrationVersion(n) {
		n.logger.Infof("%s migration already up to date!", NoSqlUpTag)
	}

	keys := make([]int, 0)

	for k := range n.migrations {
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
			if key > last {
				start = i
				break
			}
		}

		if start != 0 {
			versions = keys[start:]
		}
	}

	for _, version := range versions {
		n.logger.Infof("%s begin up migration version %d", NoSqlUpTag, version)
		script := n.migrations[version]
		n.logger.Infof("%s executing migration version %d", NoSqlUpTag, version)

		if err := script.Up(n.orm); err != nil {
			n.logger.Errorf("%s failed to execute migration up script with version %d", NoSqlUpTag, version)
			n.logger.Error(err)
			return errors.Wrapf(err, "failed to execute migration %d", version)
		}

		if _, err := n.orm.Insert(TableName, &nosqlcollection{Version: version}); err != nil {
			return errors.Wrapf(err, "failed to execute migration %d", version)
		}

		n.logger.Infof("%s migration with version %d migrated!", NoSqlUpTag, version)
	}
	return nil
}

func (n *nosql) Down() error {
	if isNoSqlMigrationsEmpty(n) {
		return nil
	}

	migrated := make([]nosqlcollection, 0)

	// - get all migration from database
	callback := func(cursor mongo.Cursor, err error) error {
		if err != nil {
			return errors.Wrap(err, "")
		}

		for cursor.Next(context.Background()) {
			coll := nosqlcollection{}
			if err := cursor.Decode(&coll); err != nil {
				return errors.Wrap(err, "")
			}
			migrated = append(migrated, coll)
		}
		return nil
	}

	if err := n.orm.Find(TableName, bson.D{}, callback, options.Find().SetSort(bson.D{{"version", -1}})); err != nil {
		return err
	}

	version := 0

	if len(migrated) != 0 {
		version = migrated[0].Version
	}

	if version == 0 {
		n.logger.Infof("%s migrations table is empty, nothing to do", NoSqlDownTag)
		return nil
	}

	keys := make([]int, 0)

	for k := range n.migrations {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	script := n.migrations[version]

	n.logger.Infof("%s begin down migration %d version", NoSqlDownTag, version)

	if err := script.Down(n.orm); err != nil {
		n.logger.Errorf("%s failed to execute migration down script with version %d", NoSqlDownTag, version)
		return errors.Wrapf(err, "failed to execute migration down script with version %d", version)
	}

	if err := n.orm.DeleteMany(TableName, bson.D{{"version", version}}); err != nil {
		n.logger.Errorf("%s failed to execute migration down script with version %d", NoSqlDownTag, version)
		return errors.Wrapf(err, "failed to execute migration down script with version %d", version)
	}

	n.logger.Infof("%s migration version %d succeeded", NoSqlDownTag, version)
	return nil
}

func (n *nosql) Check() error {
	if isNoSqlMigrationsEmpty(n) {
		return nil
	}

	versions := make([]int, 0)
	notMigrated := make([]int, 0)
	migrated := make([]nosqlcollection, 0)

	for k := range n.migrations {
		versions = append(versions, k)
	}

	err := n.orm.Find(TableName, bson.D{}, func(cursor mongo.Cursor, err error) error {
		if err != nil {
			return err
		}

		for cursor.Next(context.Background()) {
			coll := nosqlcollection{}

			if err = cursor.Decode(&coll); err != nil {
				return err
			}

			migrated = append(migrated, coll)
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "failed to iterate cursor!")
	}

	if len(migrated) == 0 && len(versions) != 0 {
		return errors.New(fmt.Sprintf("migration with version %v is not migrated!", versions))
	}

	// - find the difference
	for _, v := range versions {
		if !contains(v, migrated) {
			notMigrated = append(notMigrated, v)
		}
	}

	if len(notMigrated) > 0 {
		return errors.New(fmt.Sprintf("migration with version %v is not migrated!", notMigrated))
	}

	return nil
}

func (n *nosql) Truncate() error {
	if err := n.orm.DeleteMany(TableName, bson.D{}); err != nil {
		return errors.Wrap(err, "failed to truncate nosql migrations collection")
	}

	return nil
}

func (n *nosql) Initialize() error {
	return errors.New("initialize not implemented in nosql!")
}

// - private

func isNoSqlMigrationsEmpty(n *nosql) bool {
	length := len(n.migrations)

	if length == 0 {
		n.logger.Info("migration script is empty, nothing to migrate!")
		return true
	}

	return false
}

func getLatestNoSqlMigrationVersion(n *nosql) int {
	keys := make([]int, 0)

	for k := range n.migrations {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	return keys[len(keys)-1]
}

func contains(value int, slices []nosqlcollection) bool {
	exists := false

	for _, element := range slices {
		if value == element.Version {
			exists = true
			break
		}
	}

	return exists
}
