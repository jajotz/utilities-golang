package migration

import (
	"utilities-golang/logs"
	"utilities-golang/persistent"
	"utilities-golang/persistent/mongo"
)

const (
	TableName  = "migrations"
	ColumnName = "version"
	UpTag      = "[MIGRATION-UP] -"
	DownTag    = "[MIGRATION-DOWN] -"

	NoSqlUpTag   = "[MIGRATION-UP-NOSQL] -"
	NoSqlDownTag = "[MIGRATION-DOWN-NOSQL] -"
)

type (
	Tool interface {
		Up() error
		Down() error
		Check() error
		Truncate() error
		Initialize() error
	}

	Script struct {
		Up, Down string
	}

	NoSqlScript struct {
		Up, Down func(mongo.Mongo) error
	}

	sql struct {
		orm        persistent.ORM
		migrations map[int]*Script
		logger     logs.Logger
	}

	nosql struct {
		orm        mongo.Mongo
		migrations map[int]*NoSqlScript
		logger     logs.Logger
	}

	nosqlcollection struct {
		Version int `bson:"version"`
	}
)
